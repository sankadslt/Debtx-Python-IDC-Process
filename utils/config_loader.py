from pathlib import Path
import configparser
import threading

class ConfigLoader:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(ConfigLoader, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        config = configparser.ConfigParser()

        # Find project root (you can adjust if needed)
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]
        core_config_file_path = project_root / "Config" / "coreConfig.ini"

        if not core_config_file_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {core_config_file_path}")

        config.read(core_config_file_path)

        # Environment
        self.environment = config.get("ENVIRONMENT", "DATABASE")

        # MongoDB URI and DB name
        mongo_uri_with_db_name = config.get("MONGODB", self.environment)
        self.mongo_uri, self.database_name = mongo_uri_with_db_name.rsplit("/", 1)

        # API Endpoints
        self.case_distribution_to_drc_endpoint = config.get("API_ENDPOINTS", "Case_Distribution_To_DRC")

        # Template Task ID
        self.template_task_id = int(config.get("BatchApprovedCaseDistributedToDRCTemplate", "Template_Task_Id"))

# Create a shared instance on import
config = ConfigLoader()
