"""
Databricks Cluster Service
--------------------------
Supports three compute modes (set COMPUTE_MODE in application.properties):

  SERVERLESS  — free tier, no cluster/warehouse management needed.
                start_cluster() is a no-op; Jobs API uses serverless compute.

  WAREHOUSE   — free tier, uses a SQL Warehouse.
                start_cluster() starts the SQL Warehouse and waits for RUNNING.
                SQL Warehouses API: /api/2.0/sql/warehouses/{id}

  CLUSTER     — paid tier, uses a classic interactive cluster.
                start_cluster() starts the cluster and waits for RUNNING.
                Clusters API: /api/2.0/clusters

Databricks REST API reference:
  https://docs.databricks.com/api/workspace/clusters
  https://docs.databricks.com/api/workspace/warehouses
"""

import time
import requests
from configs.config import Config
from configs.setup_logging import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------

class ClusterStartError(Exception):
    """Raised when compute (cluster or warehouse) cannot reach RUNNING state."""


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------

class ClusterService:
    """
    Manages Databricks compute startup across three modes:
    SERVERLESS, WAREHOUSE, or CLUSTER.

    The mode is read from:
      [DATABRICKS]
      COMPUTE_MODE = SERVERLESS   (default for free tier)
    """

    # Classic cluster states
    CLUSTER_STATE_RUNNING     = "RUNNING"
    CLUSTER_STATE_TERMINATED  = "TERMINATED"
    CLUSTER_STATE_TERMINATING = "TERMINATING"
    CLUSTER_STATE_ERROR       = "ERROR"
    CLUSTER_STATE_UNKNOWN     = "UNKNOWN"

    # SQL Warehouse states
    WAREHOUSE_STATE_RUNNING  = "RUNNING"
    WAREHOUSE_STATE_STOPPED  = "STOPPED"
    WAREHOUSE_STATE_STARTING = "STARTING"
    WAREHOUSE_STATE_STOPPING = "STOPPING"
    WAREHOUSE_STATE_DELETING = "DELETING"
    WAREHOUSE_STATE_DELETED  = "DELETED"

    def __init__(self):
        cfg = Config()
        host = cfg.get(section="DATABRICKS", option="HOST", fallback="").rstrip("/")
        token = cfg.get(section="DATABRICKS", option="TOKEN", fallback="")
        self._compute_mode = cfg.get(
            section="DATABRICKS", option="COMPUTE_MODE", fallback="SERVERLESS"
        ).upper()

        if not host or not token:
            raise ClusterStartError(
                "DATABRICKS HOST and TOKEN must be set in application.properties "
                "under the [DATABRICKS] section."
            )

        self._host = host
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._max_retries = int(
            cfg.get(section="TIMEOUTS", option="CLUSTER_START_MAX_RETRIES", fallback=40)
        )
        self._retry_wait = int(
            cfg.get(section="TIMEOUTS", option="CLUSTER_START_RETRY_WAIT_SECONDS", fallback=15)
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_cluster(self, resource_id: str) -> None:
        """
        Ensure compute is ready. Behaviour depends on COMPUTE_MODE:

          SERVERLESS → no-op (compute is always available).
          WAREHOUSE  → start the SQL Warehouse and wait for RUNNING state.
          CLUSTER    → start the classic cluster and wait for RUNNING state.

        Args:
            resource_id: cluster ID (CLUSTER mode) or warehouse ID (WAREHOUSE mode).
                         Ignored in SERVERLESS mode.

        Raises:
            ClusterStartError on failure or timeout.
        """
        if self._compute_mode == "SERVERLESS":
            logger.info("COMPUTE_MODE=SERVERLESS — no cluster/warehouse to start.")
            return

        if self._compute_mode == "WAREHOUSE":
            logger.info(f"COMPUTE_MODE=WAREHOUSE — ensuring warehouse {resource_id} is running.")
            self._ensure_warehouse_running(resource_id)
            return

        if self._compute_mode == "CLUSTER":
            logger.info(f"COMPUTE_MODE=CLUSTER — ensuring cluster {resource_id} is running.")
            self._ensure_cluster_running(resource_id)
            return

        raise ClusterStartError(
            f"Unknown COMPUTE_MODE '{self._compute_mode}'. "
            "Valid values: SERVERLESS, WAREHOUSE, CLUSTER."
        )

    def get_cluster_state(self, cluster_id: str) -> str:
        """Return state of a classic cluster (CLUSTER mode only)."""
        url = f"{self._host}/api/2.0/clusters/get"
        response = requests.get(url, headers=self._headers, params={"cluster_id": cluster_id})
        self._raise_for_status(response, context=f"get_cluster_state {cluster_id}")
        return response.json().get("state", self.CLUSTER_STATE_UNKNOWN)

    def get_warehouse_state(self, warehouse_id: str) -> str:
        """Return state of a SQL Warehouse (WAREHOUSE mode only)."""
        url = f"{self._host}/api/2.0/sql/warehouses/{warehouse_id}"
        response = requests.get(url, headers=self._headers)
        self._raise_for_status(response, context=f"get_warehouse_state {warehouse_id}")
        return response.json().get("state", "UNKNOWN")

    # ------------------------------------------------------------------
    # Private — Warehouse helpers
    # ------------------------------------------------------------------

    def _ensure_warehouse_running(self, warehouse_id: str) -> None:
        state = self.get_warehouse_state(warehouse_id)
        logger.info(f"Warehouse {warehouse_id} current state: {state}")

        if state == self.WAREHOUSE_STATE_RUNNING:
            logger.info(f"Warehouse {warehouse_id} is already RUNNING.")
            return

        if state in (self.WAREHOUSE_STATE_DELETING, self.WAREHOUSE_STATE_DELETED):
            raise ClusterStartError(
                f"Warehouse {warehouse_id} is in state '{state}' and cannot be started."
            )

        if state == self.WAREHOUSE_STATE_STOPPED:
            logger.info(f"Starting warehouse {warehouse_id} ...")
            start_url = f"{self._host}/api/2.0/sql/warehouses/{warehouse_id}/start"
            response = requests.post(start_url, headers=self._headers)
            self._raise_for_status(response, context=f"start_warehouse {warehouse_id}")

        # Poll until RUNNING
        for attempt in range(1, self._max_retries + 1):
            time.sleep(self._retry_wait)
            state = self.get_warehouse_state(warehouse_id)
            logger.info(
                f"Warehouse {warehouse_id} state (attempt {attempt}/{self._max_retries}): {state}"
            )
            if state == self.WAREHOUSE_STATE_RUNNING:
                logger.info(f"Warehouse {warehouse_id} is RUNNING.")
                return
            if state in (self.WAREHOUSE_STATE_DELETING, self.WAREHOUSE_STATE_DELETED):
                raise ClusterStartError(
                    f"Warehouse {warehouse_id} entered unexpected state '{state}'."
                )

        raise ClusterStartError(
            f"Warehouse {warehouse_id} did not reach RUNNING after "
            f"{self._max_retries} retries."
        )

    # ------------------------------------------------------------------
    # Private — Classic Cluster helpers
    # ------------------------------------------------------------------

    def _ensure_cluster_running(self, cluster_id: str) -> None:
        state = self.get_cluster_state(cluster_id)
        logger.info(f"Cluster {cluster_id} current state: {state}")

        if state == self.CLUSTER_STATE_RUNNING:
            logger.info(f"Cluster {cluster_id} is already RUNNING.")
            return

        if state in (self.CLUSTER_STATE_TERMINATING, self.CLUSTER_STATE_ERROR,
                     self.CLUSTER_STATE_UNKNOWN):
            raise ClusterStartError(
                f"Cluster {cluster_id} is in state '{state}' and cannot be started."
            )

        if state == self.CLUSTER_STATE_TERMINATED:
            logger.info(f"Starting cluster {cluster_id} ...")
            start_url = f"{self._host}/api/2.0/clusters/start"
            response = requests.post(
                start_url, headers=self._headers, json={"cluster_id": cluster_id}
            )
            self._raise_for_status(response, context=f"start_cluster {cluster_id}")

        # Poll until RUNNING
        for attempt in range(1, self._max_retries + 1):
            time.sleep(self._retry_wait)
            state = self.get_cluster_state(cluster_id)
            logger.info(
                f"Cluster {cluster_id} state (attempt {attempt}/{self._max_retries}): {state}"
            )
            if state == self.CLUSTER_STATE_RUNNING:
                logger.info(f"Cluster {cluster_id} is RUNNING.")
                return
            if state in (self.CLUSTER_STATE_ERROR, self.CLUSTER_STATE_UNKNOWN,
                         self.CLUSTER_STATE_TERMINATED):
                raise ClusterStartError(
                    f"Cluster {cluster_id} entered unexpected state '{state}'."
                )

        raise ClusterStartError(
            f"Cluster {cluster_id} did not reach RUNNING after {self._max_retries} retries."
        )

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_for_status(response: requests.Response, context: str = "") -> None:
        if not response.ok:
            raise ClusterStartError(
                f"Databricks API error [{context}]: "
                f"HTTP {response.status_code} - {response.text}"
            )
