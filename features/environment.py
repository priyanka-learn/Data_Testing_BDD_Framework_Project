import logging
import sys

from configs.config import Config
from services.databricks_cluster_service import ClusterService, ClusterStartError

logger = logging.getLogger(__name__)


def before_feature(context, feature):
    """
    Runs once before each feature file.

    Reads CLUSTER_TYPE from application.properties under [DATABRICKS] and starts
    the appropriate compute resource (cluster / warehouse / no-op for serverless).

    To change the cluster type, update CLUSTER_TYPE in application.properties:
        CLUSTER_TYPE=DEFAULT    ← standard
        CLUSTER_TYPE=STREAMING  ← if you have a separate streaming cluster/warehouse
    """
    config = Config()

    # Read cluster type from application.properties (defaults to DEFAULT)
    cluster_key = config.get_cluster_type()

    cluster_id = config.get_cluster_id(cluster_key)

    cluster_service = ClusterService()
    try:
        logger.info(f"Starting cluster '{cluster_key}' for feature '{feature.name}' ...")
        cluster_service.start_cluster(cluster_id)
        logger.info(f"Cluster '{cluster_key}' started.")
    except ClusterStartError as e:
        logger.error(f"Failed to start cluster '{cluster_key}' for feature '{feature.name}': {e}")
        raise


def before_all(context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove any existing handlers Behave may have added
    logger.handlers.clear()

    # Add a fresh handler that writes to console
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)