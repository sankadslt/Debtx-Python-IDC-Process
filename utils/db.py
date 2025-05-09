from pymongo import MongoClient
from utils.coreConfig import config
import atexit

class MongoDB:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        mongo_uri = config.get("mongo_uri")
        database_name = config.get("database_name")

        if not mongo_uri or not database_name:
            raise ValueError("MongoDB URI or database name not configured properly.")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]

        # Automatically close MongoDB connection on program exit
        atexit.register(self.close)

    def get_db(self):
        return self.db

    def close(self):
        if hasattr(self, 'client'):
            self.client.close()
            print("[MongoDB] Connection closed.")

# Singleton instance you can import anywhere
db = MongoDB().get_db()
