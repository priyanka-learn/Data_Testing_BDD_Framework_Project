from configparser import ConfigParser, NoSectionError
import os

from behave.exception import ConfigError


class Config:
    _instance = None

    def __new__(cls, config_file: str = None):
        """Ensure single instance of Config."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._properties = None
            cls._instance._root_dir = None
            cls._instance._load_config(config_file)
        return cls._instance

    def get_root_directory(self) -> str:
        """Return the detected project root directory."""
        if self._root_dir is None:
            self._root_dir = self._get_root_directory()
        return self._root_dir

    def _load_config(self, config_file: str = None):
        """Load configuration from file."""
        if self._properties is None:
            self._properties = self._initialize_config_parser()

            # Determine config file path
            config_file_name = config_file or self._get_config_file_path()

            # Load the configuration file
            self._load_config_file(config_file_name)

    def _get_config_file_path(self, markers=None):
        """Determine the path to the configuration file located at the project root."""
        root_dir = self.get_root_directory()
        config_file_path = os.path.join(root_dir, "application.properties")

        # Ensure the file exists
        if not os.path.exists(config_file_path):
            print(f"Config file {config_file_path} not found")
            raise ConfigError(f"Config file {config_file_path} not found")
        print(f"Found Config file at: {config_file_path}")
        return config_file_path

    def _get_root_directory(self, markers=None):
        """Find the root directory based on project markers.

        Markers can be a list of filenames that identify the project root.
        The first directory containing any marker is treated as root.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Default markers (module path + common project files)
        if markers is None:
            markers = ["application.properties", "Pipfile"]

        while current_dir and not any(
            os.path.exists(os.path.join(current_dir, marker)) for marker in markers
        ):
            parent_dir = os.path.dirname(current_dir)
            # Check if we've reached the filesystem root
            if parent_dir == current_dir:
                break
            current_dir = parent_dir

        if not any(
            os.path.exists(os.path.join(current_dir, marker)) for marker in markers
        ):
            print("Error! Root directory not found")
            raise ConfigError("Root directory not found")

        print(f"Root directory found at: {current_dir}")
        return current_dir

    def _initialize_config_parser(self):
        """Initialize and return a ConfigParser instance."""
        return ConfigParser(allow_no_value=True)

    def _load_config_file(self, config_file_path: str):
        """Load the configuration file into the parser."""
        try:
            self._properties.read(config_file_path)
        except Exception as e:
            print(f"Failed to load config file {config_file_path}: {e}")
            raise

    def get(self, section, option, fallback=None):
        """Access configuration option"""
        try:
            return self._properties.get(section, option)
        except NoSectionError:
            print("Warning! Logging not found")
        except Exception as e:
            print(
                "Warning! Error accessing Option in Section : Returning Fallback value :"
            )
        return fallback

    def get_folder(self, key: str) -> str:
        """Get folder name value by key from the [FOLDERS] section."""
        return self.get("FOLDERS", key)

    # ── Databricks connection ────────────────────────────────────────────────

    def get_host(self) -> str:
        """Get the Databricks workspace hostname (strips https:// prefix if present)."""
        return self.get("DATABRICKS", "HOST", fallback="").strip().replace("https://", "")

    def get_token(self) -> str:
        """Get the Databricks personal access token."""
        return self.get("DATABRICKS", "TOKEN", fallback="").strip()

    def get_cluster_type(self) -> str:
        """Get the configured cluster/warehouse type key (e.g. DEFAULT, STREAMING)."""
        return self.get("DATABRICKS", "CLUSTER_TYPE", fallback="DEFAULT").strip().upper()

    # ── Compute resources ────────────────────────────────────────────────────

    def get_compute_mode(self) -> str:
        """
        Return the configured compute mode (SERVERLESS, WAREHOUSE, or CLUSTER).
        Defaults to SERVERLESS if not set (safe default for free tier).
        """
        return self.get("DATABRICKS", "COMPUTE_MODE", fallback="SERVERLESS").upper()

    def get_cluster_id(self, cluster_type: str) -> str:
        """
        Return the correct compute resource ID based on COMPUTE_MODE.

        - SERVERLESS → returns empty string (no ID needed).
        - WAREHOUSE  → reads from [WAREHOUSES] section.
        - CLUSTER    → reads from [CLUSTERS] section.

        Args:
            cluster_type: logical name, e.g. 'DEFAULT'  (case-insensitive)
        """
        mode = self.get_compute_mode()

        if mode == "SERVERLESS":
            return ""

        if mode == "WAREHOUSE":
            resource_id = self.get("WAREHOUSES", cluster_type.upper())
            if not resource_id:
                raise ConfigError(
                    f"Warehouse ID for type '{cluster_type}' not found in [WAREHOUSES] "
                    f"section of application.properties."
                )
            return resource_id

        # CLUSTER mode
        resource_id = self.get("CLUSTERS", cluster_type.upper())
        if not resource_id:
            raise ConfigError(
                f"Cluster ID for type '{cluster_type}' not found in [CLUSTERS] section "
                f"of application.properties."
            )
        return resource_id

    def get_warehouse_id(self, warehouse_type: str = None) -> str:
        """
        Get a SQL Warehouse ID by logical type name from [WAREHOUSES] section.
        If warehouse_type is not provided, uses the CLUSTER_TYPE from config.

        How to find your Warehouse ID:
          SQL Warehouses → click warehouse → Connection Details → HTTP Path
          e.g. /sql/1.0/warehouses/abc123def456  → ID is 'abc123def456'
        """
        if warehouse_type is None:
            warehouse_type = self.get_cluster_type()
        warehouse_id = self.get("WAREHOUSES", warehouse_type.upper())
        if not warehouse_id:
            raise ConfigError(
                f"Warehouse ID for type '{warehouse_type}' not found in [WAREHOUSES] "
                f"section of application.properties."
            )
        return warehouse_id

    # ── Unity Catalog ────────────────────────────────────────────────────────

    def get_catalog_name(self) -> str:
        """Get the Databricks Unity Catalog name."""
        return self.get("DATABRICKS", "CATALOG_NAME", fallback="")

    def get_database_name(self) -> str:
        """Get the Databricks database/schema name."""
        return self.get("DATABRICKS", "DATABASE_NAME", fallback="")
