"""
Databricks Job Service
----------------------
Handles all REST API calls related to Databricks Jobs / Runs.
Supports three compute modes (configured via COMPUTE_MODE in application.properties):

  SERVERLESS  — submit notebook runs without specifying any cluster.
                Databricks automatically provisions serverless compute.
                (Works on free tier)

  WAREHOUSE   — submit notebook runs on a SQL Warehouse.
                (Works on free tier for SQL notebooks)

  CLUSTER     — submit notebook runs on a classic interactive cluster.
                (Paid tier only)

Databricks REST API reference:
  https://docs.databricks.com/api/workspace/jobs
"""

import requests
from typing import Dict, Tuple, Optional
from configs.config import Config
from configs.setup_logging import get_logger

logger = get_logger()


class JobServiceError(Exception):
    """Raised when a Databricks Jobs API call fails."""


class JobService:
    """
    Wraps the Databricks Jobs REST API (v2.1).

    Configuration keys read from application.properties:
      [DATABRICKS]
      HOST         = https://<workspace>.azuredatabricks.net
      TOKEN        = dapi<token>
      COMPUTE_MODE = SERVERLESS | WAREHOUSE | CLUSTER
    """

    def __init__(self):
        cfg = Config()
        host = cfg.get(section="DATABRICKS", option="HOST", fallback="").rstrip("/")
        token = cfg.get(section="DATABRICKS", option="TOKEN", fallback="")
        self._compute_mode = cfg.get(
            section="DATABRICKS", option="COMPUTE_MODE", fallback="SERVERLESS"
        ).upper()

        if not host or not token:
            raise JobServiceError(
                "DATABRICKS HOST and TOKEN must be set in application.properties "
                "under the [DATABRICKS] section."
            )

        self._runs_url = f"{host}/api/2.1/jobs/runs"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit_run(
        self,
        cluster_id: str,
        notebook_path: str,
        base_parameters: Dict = None,
    ) -> Tuple[Optional[int], str]:
        """
        Submit a one-time notebook run.

        Behaviour depends on COMPUTE_MODE:
          SERVERLESS → ignores cluster_id, uses serverless compute automatically.
          WAREHOUSE  → uses cluster_id as a SQL Warehouse ID.
          CLUSTER    → uses cluster_id as a classic cluster ID.

        Args:
            cluster_id:       Cluster or Warehouse ID (ignored in SERVERLESS mode).
            notebook_path:    Absolute path in the Databricks workspace.
            base_parameters:  Key-value pairs passed to the notebook as widgets.

        Returns:
            (run_id, message)  on success — run_id is an integer.
            (None, message)    on failure.
        """
        payload = self._build_submit_payload(cluster_id, notebook_path, base_parameters or {})

        url = f"{self._runs_url}/submit"
        logger.info(
            f"Submitting notebook run [{self._compute_mode}]: {notebook_path}"
            + (f" on {cluster_id}" if self._compute_mode != "SERVERLESS" else " (serverless)")
        )

        try:
            response = requests.post(url, headers=self._headers, json=payload)
            self._raise_for_status(response, context="submit_run")
            data = response.json()
            run_id = data.get("run_id")
            message = f"Notebook '{notebook_path}' submitted successfully. run_id={run_id}"
            logger.info(message)
            return run_id, message
        except JobServiceError as e:
            logger.error(str(e))
            return None, str(e)

    def get_job_run_state(self, run_id: int) -> Dict:
        """
        Return the state dict for a run.

        Returns a dict with at least:
          {
            "life_cycle_state": "RUNNING" | "TERMINATED" | ...,
            "result_state":     "SUCCESS" | "FAILED" | ...,  # only when TERMINATED
            "state_message":    "<optional detail>",
          }
        """
        url = f"{self._runs_url}/get"
        response = requests.get(url, headers=self._headers, params={"run_id": run_id})
        self._raise_for_status(response, context=f"get_job_run_state run_id={run_id}")
        return response.json().get("state", {})

    def cancel_run(self, run_id: int) -> Dict:
        """
        Cancel an active run.

        Returns:
            {'status': 'SUCCESS'} on success, or a dict with error details.
        """
        url = f"{self._runs_url}/cancel"
        logger.info(f"Cancelling run ID: {run_id}")
        try:
            response = requests.post(url, headers=self._headers, json={"run_id": run_id})
            self._raise_for_status(response, context=f"cancel_run run_id={run_id}")
            logger.info(f"Run {run_id} cancelled successfully.")
            return {"status": "SUCCESS"}
        except JobServiceError as e:
            logger.error(str(e))
            return {"status": "FAILED", "error": str(e)}

    def list_active_runs(self) -> Dict:
        """
        List all currently active (RUNNING / PENDING) runs.

        Returns:
            Databricks API response dict, e.g. {'runs': [...]}
        """
        url = f"{self._runs_url}/list"
        response = requests.get(
            url,
            headers=self._headers,
            params={"active_only": "true", "limit": 100},
        )
        self._raise_for_status(response, context="list_active_runs")
        return response.json()

    def get_run_details(self, run_id: int) -> Dict:
        """Return full details of a run (cluster_instance, task, etc.)."""
        url = f"{self._runs_url}/get"
        response = requests.get(url, headers=self._headers, params={"run_id": run_id})
        self._raise_for_status(response, context=f"get_run_details run_id={run_id}")
        return response.json()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_submit_payload(
        self, cluster_id: str, notebook_path: str, base_parameters: Dict
    ) -> Dict:
        """Build the correct Jobs API payload based on COMPUTE_MODE."""

        notebook_task = {
            "notebook_path": notebook_path,
            "base_parameters": base_parameters,
        }

        if self._compute_mode == "SERVERLESS":
            # No cluster specification — Databricks uses serverless compute automatically.
            return {
                "run_name": "bdd_run",
                "tasks": [
                    {
                        "task_key": "notebook",
                        "notebook_task": notebook_task,
                    }
                ],
                "queue": {"enabled": True},
            }

        if self._compute_mode == "WAREHOUSE":
            # SQL Warehouse: attach the warehouse to the notebook task.
            notebook_task["warehouse_id"] = cluster_id
            return {
                "run_name": "bdd_run",
                "tasks": [
                    {
                        "task_key": "notebook",
                        "notebook_task": notebook_task,
                    }
                ],
            }

        # CLUSTER mode — classic existing_cluster_id (original behaviour).
        return {
            "run_name": "bdd_run",
            "existing_cluster_id": cluster_id,
            "notebook_task": notebook_task,
        }

    @staticmethod
    def _raise_for_status(response: requests.Response, context: str = "") -> None:
        if not response.ok:
            raise JobServiceError(
                f"Databricks API error [{context}]: "
                f"HTTP {response.status_code} - {response.text}"
            )
