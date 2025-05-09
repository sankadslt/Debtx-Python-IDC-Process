from pathlib import Path
import configparser

class CoreConfig:
    _instance = None
    _config_values = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CoreConfig, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        config = configparser.ConfigParser()
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]
        core_config_file_path = project_root / "Config" / "coreConfig.ini"

        if not core_config_file_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {core_config_file_path}")

        config.read(core_config_file_path)

        # ENVIRONMENT
        extracted_environment = config.get("ENVIRONMENT", "DATABASE")
        self._config_values["environment"] = extracted_environment

        mongo_uri_with_db_name = config.get("MONGODB", extracted_environment)
        if not mongo_uri_with_db_name.strip():
            raise ValueError(f"{extracted_environment} MongoDB URI is empty or null.")

        mongo_uri, database_name = mongo_uri_with_db_name.rsplit("/", 1)
        self._config_values["mongo_uri"] = mongo_uri
        self._config_values["database_name"] = database_name

        # STATIC VALUES (renamed from STATIC_VALUES)
        try:
            upload_task_number = config.get("TEMPLATE_TASK_ID", "DATA_UPLOAD_FROM_FILE_TASK_NUMBER")
            max_monitor_months = config.get("TEMPLATE_TASK_ID", "MAXIMUM_NO_OF_MONITOR_MONTHS")

            self._config_values["upload_task_number"] = [int(upload_task_number)]
            self._config_values["max_monitor_months"] = int(max_monitor_months)
        except Exception as e:
            raise Exception("Missing static values or incorrect formatting in coreConfig.ini") from e

        # API_ENDPOINTS
        self._config_values["get_case_phase_endpoint"] = config.get("API_ENDPOINTS", "GET_CASE_PHASE")
        self._config_values["incident_creation_endpoint"] = config.get("API_ENDPOINTS", "INCIDENT_CREATION")

       # File paths
        self._config_values["uploaded_file_saved_dir"] = config.get("PATHS", "UPLOADED_FILE_SAVED_DIR")
        self._config_values["validity_period_extend_path"] = config.get("PATHS", "VALIDITY_PERIOD_EXTEND_PATH")
        self._config_values["incident_creation_path"] = config.get("PATHS", "INCIDENT_CREATION_PATH")
        self._config_values["incident_reject_path"] = config.get("PATHS", "INCIDENT_REJECT_PATH")
        self._config_values["case_hold_path"] = config.get("PATHS", "CASE_HOLD_PATH")
        self._config_values["case_discard_path"] = config.get("PATHS", "CASE_DISCARD_PATH")
        
    def get(self, key, default=None):
        return self._config_values.get(key, default)

    def all(self):
        return self._config_values


# Usage: Import this singleton from anywhere like this:
# from coreconfig import config
config = CoreConfig()
