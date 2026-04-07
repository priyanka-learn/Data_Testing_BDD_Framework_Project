"""
Databricks Workspace Service
-----------------------------
Handles REST API calls related to the Databricks Workspace:
  - Checking whether a notebook / directory path exists

Databricks REST API reference:
  https://docs.databricks.com/api/workspace/workspace
"""

import requests
from configs.config import Config
from configs.setup_logging import get_logger

logger = get_logger()


class WorkspaceServiceError(Exception):
    """Raised when a Databricks Workspace API call fails unexpectedly."""


class WorkspaceService:
    """
    Wraps the Databricks Workspace REST API (v2.0).

    Configuration keys read from application.properties:
      [DATABRICKS]
      HOST  = https://<your-workspace>.azuredatabricks.net
      TOKEN = dapi<your-personal-access-token>
    """

    def __init__(self):
        cfg = Config()
        host = cfg.get(section="DATABRICKS", option="HOST", fallback="").rstrip("/")
        token = cfg.get(section="DATABRICKS", option="TOKEN", fallback="")

        if not host or not token:
            raise WorkspaceServiceError(
                "DATABRICKS HOST and TOKEN must be set in application.properties "
                "under the [DATABRICKS] section."
            )

        self._base_url = f"{host}/api/2.0/workspace"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check_if_exists(self, workspace_path: str) -> bool:
        """
        Check whether a notebook or folder exists at the given workspace path.

        Args:
            workspace_path: Absolute path in the Databricks workspace,
                            e.g. '/Users/me@example.com/my_notebook'

        Returns:
            True  if the path exists (notebook, directory, library, etc.)
            False if the path does not exist (404) or access is denied (403)

        Raises:
            WorkspaceServiceError: on unexpected API errors (5xx, etc.)
        """
        url = f"{self._base_url}/get-status"
        logger.info(f"Checking workspace path: {workspace_path}")

        response = requests.get(
            url,
            headers=self._headers,
            params={"path": workspace_path},
        )

        if response.status_code == 200:
            logger.info(f"Path exists: {workspace_path}")
            return True

        if response.status_code in (404, 403):
            logger.warning(
                f"Path not found or access denied (HTTP {response.status_code}): {workspace_path}"
            )
            return False

        # Any other non-2xx is unexpected
        raise WorkspaceServiceError(
            f"Databricks Workspace API error [check_if_exists]: "
            f"HTTP {response.status_code} - {response.text}"
        )

    def list_workspace(self, workspace_path: str) -> dict:
        """
        List the contents of a workspace directory.

        Args:
            workspace_path: Absolute path to a directory in the workspace.

        Returns:
            Databricks API response dict, e.g. {'objects': [...]}

        Raises:
            WorkspaceServiceError: on API errors.
        """
        url = f"{self._base_url}/list"
        response = requests.get(
            url,
            headers=self._headers,
            params={"path": workspace_path},
        )

        if not response.ok:
            raise WorkspaceServiceError(
                f"Databricks Workspace API error [list_workspace]: "
                f"HTTP {response.status_code} - {response.text}"
            )

        return response.json()
