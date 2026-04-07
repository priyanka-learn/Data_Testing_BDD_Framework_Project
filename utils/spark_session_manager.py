"""
Spark Session Manager
---------------------
Creates and returns a shared local SparkSession.

On a free Databricks account (no cluster, no Databricks Connect serverless),
data is read from the SQL Warehouse via databricks-sql-connector and converted
into a PySpark DataFrame using a local SparkSession.

Usage:
    from utils.spark_session_manager import SparkSessionManager

    spark = SparkSessionManager.get_spark_session()
    df = spark.createDataFrame(pandas_df)
"""

import os
import sys

from pyspark.sql import SparkSession
from configs.setup_logging import get_logger

logger = get_logger()


class SparkSessionManager:
    """Singleton wrapper around a local SparkSession."""

    _spark = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        """Return a shared local SparkSession, creating it on first call."""
        if cls._spark is None:
            # Point Spark workers at the same Python interpreter that is
            # running this process so they don't fall back to the system
            # PATH (which on Windows hits the broken MS Store alias).
            python_exec = sys.executable
            os.environ["PYSPARK_PYTHON"] = python_exec
            os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

            # On Windows, PySpark requires Hadoop binaries (winutils.exe).
            # Set HADOOP_HOME if not already set.
            if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
                hadoop_home = r"C:\hadoop"
                os.environ["HADOOP_HOME"] = hadoop_home
                os.environ["PATH"] = os.environ["PATH"] + f";{hadoop_home}\\bin"
                logger.info(f"Set HADOOP_HOME={hadoop_home} for Windows PySpark.")

            logger.info("Creating local SparkSession.")
            cls._spark = (
                SparkSession.builder
                .appName("BDD_Practice_Project")
                .master("local[*]")
                .config("spark.python.worker.faulthandler.enabled", "true")
                .config("spark.driver.extraJavaOptions",
                        "-Dlog4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR")
                .getOrCreate()
            )
            cls._spark.sparkContext.setLogLevel("ERROR")
            logger.info("Local SparkSession created successfully.")
        return cls._spark
