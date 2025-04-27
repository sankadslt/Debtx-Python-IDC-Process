from pymongo import MongoClient
import threading
from utils.config_loader import config  # adjust import based on your project structure

class MongoDBClient:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(MongoDBClient, cls).__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        self.client = MongoClient(config.mongo_uri)
        self.db = self.client[config.database_name]

# Shared instance
mongo_client = MongoDBClient()
db = mongo_client.db
