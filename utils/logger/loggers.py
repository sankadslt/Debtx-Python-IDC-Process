import logging
import logging.config
import os

# Ensure the logs directory exists
logs_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(logs_dir, exist_ok=True)

# Load logger configuration from loggers.ini
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_file = os.path.join(base_dir, "..", "Config", "logConfig.ini")

logging.config.fileConfig(config_file)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
