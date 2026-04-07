import threading
import time
import os
from typing import Dict, Tuple
from databricks import sql as dbsql
from pyspark.sql.types import StructType, StructField, StringType
from configs.config import Config
from configs.setup_logging import get_logger
from services.databricks_cluster_service import ClusterService, ClusterStartError
from services.databricks_job_service import JobService
from services.databricks_workspace_service import WorkspaceService
from utils.spark_session_manager import SparkSessionManager

logger = get_logger()
config = Config()
streaming_notebook_run_ids = {}
table_locks = {}
lock = threading.Lock()


class DatabricksJobExecutionError(Exception):
    """Custom exception for Databricks job execution errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DatabricksUtil:
    """
    Class: DatabricksUtil

    Purpose:
    - To provide utility methods for managing Databricks-related operations such as starting clusters, submitting notebook runs, verifying statuses, and terminating runs.

    Methods:
    - `log_and_return(message: str, success: bool = False) -> Tuple[bool, str]`: Logs a message and returns a consistent result.
    - `get_pipeline_notebook_url(notebook: str) -> str`: Generates a URL for a pipeline notebook with `CI_JOB_ID`.
    - `start_cluster_if_needed(cluster_type: str)`: Starts and verifies a Databricks cluster if needed.
    - `check_notebook_exists(notebook_path: str) -> bool`: Checks if a notebook exists in the Databricks workspace.
    - `_check_workspace_exists(notebook_path: str) -> str | bool`: Helper function to check if a notebook exists in the workspace.
    - `submit_notebook_run(notebook_path: str, cluster_id: str, base_parameters: Dict = None) -> Tuple[bool, str]`: Submits a Databricks notebook and checks the status of the run.
    - `get_run_status_verify(run_id: int, notebook: str) -> Tuple[bool, str]`: Verifies the run status of a submitted notebook.
    - `terminate_databricks_notebook(notebook: str) -> tuple[bool, str] | bool`: Terminates a Databricks notebook run using its run ID.
    - `is_notebook_running_on_cluster(cluster_id: str, notebook_name: str, specific_notebooks: List = None) -> bool`: Checks if a Databricks job running a specific notebook is active on the given cluster.

    Dependencies:
    - Standard Libraries: `os`, `time`, `threading`
    - Custom modules:
    - `configs.config` for configuration management.
    - `configs.setup_logging` for logging setup.
    - `services.databricks_cluster_service` for cluster operations.
    - `services.databricks_job_service` for job operations.
    - `services.databricks_workspace_service` for workspace operations.
    """

    STATE_INTERNAL_ERROR = 'INTERNAL_ERROR'
    STATE_SKIPPED = 'SKIPPED'
    STATE_TERMINATED = 'TERMINATED'
    STATE_SUCCESS = 'SUCCESS'

    MAX_RETRIES = int(config.get(section='TIMEOUTS', option='NOTEBOOK_STATUS_MAX_RETRIES'))
    RETRY_WAIT_SECONDS = int(config.get(section='TIMEOUTS', option='NOTEBOOK_STATUS_RETRY_WAIT_SECONDS'))

    @staticmethod
    def log_and_return(message: str, success: bool = False) -> Tuple[bool, str]:
        """ Helper function to log messages and return consistent result. """
        logger.error(message) if not success else logger.info(message)
        return success, message

    @staticmethod
    def get_pipeline_notebook_url(notebook: str) -> str:
        """Generate a URL for a pipeline notebook with CI_JOB_ID."""
        ci_job_id = os.getenv("CI_JOB_ID")
        if ci_job_id:
            return notebook.replace('/ngp-insights/', f"/ngp-insights-{ci_job_id}/")
        else:
            return DatabricksUtil.log_and_return(message="CI_JOB_ID environment variable is not set.", success=False)[1]

    @staticmethod
    def start_cluster_if_needed(cluster_type: str):
        """Start and verify a Databricks cluster if needed."""
        cluster_id = Config().get_cluster_id(cluster_type)
        cluster_service = ClusterService()
        try:
            logger.info(f"Starting and verifying cluster of type {cluster_type} with ID: {cluster_id}")
            cluster_service.start_cluster(cluster_id)
        except ClusterStartError as e:
            return DatabricksUtil.log_and_return(message=f"Cluster {cluster_id} failed to start or verify: {e}", success=False)

    @staticmethod
    def check_notebook_exists(notebook_path: str) -> bool:
        """Checks if a notebook exists in the Databricks workspace."""
        return DatabricksUtil._check_workspace_exists(notebook_path)

    @staticmethod
    def _check_workspace_exists(notebook_path: str) -> str | bool:
        """Helper function to check if a notebook exists in the workspace."""
        try:
            workspace_service = WorkspaceService()
            exists = workspace_service.check_if_exists(workspace_path=notebook_path)
            if exists:
                logger.info(f"Notebook found at path: {notebook_path}")
            else:
                logger.warning(f"Notebook not found at path: {notebook_path}")
            return exists
        except Exception as e:
            return DatabricksUtil.log_and_return(message=f"Error verifying notebook: {notebook_path}. Error: {e}", success=False)[1]

    @staticmethod
    def submit_notebook_run(
            notebook_path: str,
            cluster_id: str,
            base_parameters: Dict = None
    ) -> Tuple[bool, str]:
        """Submits a Databricks notebook and checks the status of the run."""
        base_parameters = base_parameters or {}
        job_service = JobService()

        try:
            run_id, message = job_service.submit_run(cluster_id, notebook_path, base_parameters)
            if run_id:
                streaming_notebook_run_ids[notebook_path] = run_id
                logger.info(f"Notebook '{notebook_path}' started with run ID: {run_id}")
                return run_id, message
            else:
                return DatabricksUtil.log_and_return(
                    message=f"Failed to submit the run for notebook {notebook_path}: {message}", success=False)
        except Exception as e:
            return DatabricksUtil.log_and_return(
                message=f"An error occurred while executing the notebook '{notebook_path}': {e}", success=False)

    @staticmethod
    def get_run_status_verify(run_id: int, notebook: str) -> Tuple[bool, str]:
        """Verify the run status of the submitted notebook."""
        success, message = False, f'Notebook {notebook} execution has timed out'
        retry = 0

        while retry < DatabricksUtil.MAX_RETRIES and not success:
            job_service = JobService()
            state = job_service.get_job_run_state(run_id=run_id)

            life_cycle_state = state['life_cycle_state']
            result_state = state.get('result_state')

            # Get detailed error message if available
            error_message = ""
            if state.get('state_message'):
                error_message = state.get('state_message')

            if life_cycle_state == DatabricksUtil.STATE_INTERNAL_ERROR:
                message = f'Internal error on running notebook :: {notebook}'
                return DatabricksUtil.log_and_return(message, success=False)
            elif life_cycle_state == DatabricksUtil.STATE_SKIPPED:
                message = f'A previous run of the job is already running'
                return DatabricksUtil.log_and_return(message, success=False)
            elif life_cycle_state == DatabricksUtil.STATE_TERMINATED:
                if result_state == DatabricksUtil.STATE_SUCCESS:
                    success = True
                    message = f"Notebook {notebook} executed successfully."
                else:
                    message = f"Notebook :: {notebook} execution has {result_state}. Details: {error_message}"
                    return DatabricksUtil.log_and_return(message, success=False)

            time.sleep(DatabricksUtil.RETRY_WAIT_SECONDS)
            retry += 1

        return success, message

    @staticmethod
    def terminate_databricks_notebook(notebook: str) -> tuple[bool, str] | bool:
        """Terminate a Databricks notebook run using its run ID."""
        job_service = JobService()

        # Log the current state of streaming_notebook_run_ids for debugging
        logger.info(f"Current streaming_notebook_run_ids: {streaming_notebook_run_ids}")

        try:
            if notebook in streaming_notebook_run_ids:
                run_id = streaming_notebook_run_ids.pop(notebook)
                logger.info(f"Attempting to terminate run for notebook: {notebook} with run ID: {run_id}")
                termination_result = job_service.cancel_run(run_id)

                if termination_result is None:
                    return DatabricksUtil.log_and_return(message=f"Cancel run returned None for run ID {run_id}.", success=False)
                elif termination_result.get('status') == 'SUCCESS':
                    logger.info(f"Successfully terminated notebook run with ID: {run_id}")
                    return True
                else:
                    return DatabricksUtil.log_and_return(
                        message=f"Failed to terminate notebook run with ID: {run_id}. Result: {termination_result}", success=False)

            return DatabricksUtil.log_and_return(
                message=f"Run ID for notebook '{notebook}' not found in streaming_notebook_run_ids.", success=False)

        except KeyError as e:
            return DatabricksUtil.log_and_return(
                message=f"KeyError: Run ID for notebook '{notebook}' not found in streaming_notebook_run_ids.", success=False)
        except Exception as e:
            return DatabricksUtil.log_and_return(
                message=f"An unexpected error occurred while terminating notebook '{notebook}': {str(e)}", success=False)

    @staticmethod
    def is_notebook_running_on_cluster(cluster_id: str, notebook_name: str, specific_notebooks: list = None):
        """
        Check if a Databricks job running a specific notebook is active on the given cluster.
        Optionally, check for specific notebooks like 'write_to_bronze_using_dbignite' or 'write_to_silver_using_dbignite'.
        """
        try:
            job_service = JobService()
            output = job_service.list_active_runs()

            logger.debug(f"Active runs found: {output}")
            logger.debug(
                f"Looking for notebook containing '{notebook_name}' or any notebooks in {specific_notebooks} on cluster '{cluster_id}'")

            # Default to checking for bronze and silver notebooks if no specific notebooks are provided
            if specific_notebooks is None:
                specific_notebooks = ['execute_bronzeToSilver', 'write_to_bronze_using_dbignite']

            # Get just the notebook filename without full path for more flexible matching
            notebook_basename = notebook_name.split('/')[-1] if '/' in notebook_name else notebook_name

            # Check if there are any runs
            if 'runs' in output:
                for run in output['runs']:
                    if run['run_name'] == 'bdd_run':
                        run_details = job_service.get_run_details(run_id=run['run_id'])

                        logger.debug(f"Checking run ID {run['run_id']} details: {run_details}")

                        # Check cluster match
                        cluster_match = (run_details and
                                         'cluster_instance' in run_details and
                                         'cluster_id' in run_details['cluster_instance'] and
                                         run_details['cluster_instance']['cluster_id'] == cluster_id)

                        # Get notebook path from details
                        notebook_path = None
                        if (run_details and 'task' in run_details and
                                'notebook_task' in run_details['task'] and
                                'notebook_path' in run_details['task']['notebook_task']):
                            notebook_path = run_details['task']['notebook_task']['notebook_path']

                        # Check if the notebook path contains our target notebook
                        notebook_match = False
                        if notebook_path:
                            # Get the basename of the running notebook
                            running_notebook_basename = notebook_path.split('/')[
                                -1] if '/' in notebook_path else notebook_path

                            # Check if either the full path or just the basename matches
                            notebook_match = (notebook_name in notebook_path or
                                              notebook_basename == running_notebook_basename or
                                              # Check if running notebook matches any in the specific_notebooks list
                                              running_notebook_basename in specific_notebooks or
                                              # Add additional check for just the basename in specific_notebooks
                                              any(specific_nb in running_notebook_basename for specific_nb in
                                                  specific_notebooks))

                        # If both cluster and notebook match, we found a running instance
                        if cluster_match and notebook_match:
                            logger.info(
                                f"Found notebook matching '{notebook_name}' or one of the specified notebooks in {specific_notebooks} running on cluster '{cluster_id}'")
                            logger.info(f"Actual notebook path: {notebook_path}")
                            return True

            logger.info(
                f"No instances of notebook '{notebook_name}' or any specified notebooks in {specific_notebooks} found running on cluster '{cluster_id}'")
            return False
        except Exception as e:
            logger.error(f"Error while checking notebook status: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _get_warehouse_connection_params():
    """Return (host, token, http_path) for the configured SQL Warehouse."""
    host         = config.get_host()
    token        = config.get_token()
    warehouse_id = config.get_warehouse_id()
    return host, token, f"/sql/1.0/warehouses/{warehouse_id}"


def _build_full_table_name(table_name: str) -> str:
    """Return fully-qualified table name as catalog.database.table (or shorter if not configured)."""
    catalog  = config.get_catalog_name()
    database = config.get_database_name()
    if catalog and database:
        return f"{catalog}.{database}.{table_name}"
    elif database:
        return f"{database}.{table_name}"
    return table_name


def read_json_to_spark_df(path, multiline: bool = True):
    """
    Read a local JSON file into a PySpark DataFrame.

    Args:
        path: Resolved path to the JSON file.
        multiline: Set to True for pretty-printed / multi-line JSON (default).
    Returns:
        PySpark DataFrame.
    """
    spark = SparkSessionManager.get_spark_session()
    df = spark.read.option("multiline", multiline).json(str(path))
    logger.info(f"JSON file loaded as PySpark DataFrame: {path}")
    return df


def read_table_to_df(table_name: str):
    """
    Read a Databricks table via SQL Warehouse.

    Args:
        table_name: Simple table name (e.g. 'fhir_patients').
    Returns:
        List of dicts, one per row, with all column values cast to str.
    """
    host, token, http_path = _get_warehouse_connection_params()
    full_table = _build_full_table_name(table_name)

    logger.info(f"Fetching Databricks table via SQL Warehouse: {full_table}")

    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            # Fetch column names first via a zero-row query
            cursor.execute(f"SELECT * FROM {full_table} LIMIT 0")
            col_names = [desc[0] for desc in cursor.description]

            # Cast every column to STRING in SQL — avoids all arrow/pyarrow type conversion
            # issues (out-of-range dates, nested timestamps in structs, tz lookups, etc.)
            cast_select = ", ".join(f"CAST(`{c}` AS STRING) AS `{c}`" for c in col_names)
            cursor.execute(f"SELECT {cast_select} FROM {full_table}")
            rows = cursor.fetchall()

    result = [dict(zip(col_names, row)) for row in rows]
    logger.info(f"Table '{full_table}' loaded as list of {len(result)} rows.")
    return result


def get_table_row_count(table_name: str) -> int:
    """
    Return the row count of a Databricks table via SQL Warehouse.
    Uses COUNT(*) directly in SQL — avoids fetching all rows just to count them.

    Args:
        table_name: Simple table name (e.g. 'fhir_patients').
    Returns:
        Integer row count.
    """
    host, token, http_path = _get_warehouse_connection_params()
    full_table = _build_full_table_name(table_name)

    logger.info(f"Fetching row count for Databricks table: {full_table}")

    with dbsql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
            count = cursor.fetchone()[0]

    logger.info(f"Table '{full_table}' row count: {count}")
    return count