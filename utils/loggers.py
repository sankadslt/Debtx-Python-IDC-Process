import logging
import logging.config
import os
from utils.filePath import get_filePath
# Load logger configuration from loggers.ini
config_file = get_filePath("loggers")

logging.debug(f"Logger configuration file resolved to: {config_file}")  # Debug log for resolved path
if not os.path.isfile(config_file):
    raise FileNotFoundError(f"Logger configuration file not found: {config_file}")

logging.config.fileConfig(config_file)

def get_logger(name: str) -> logging.Logger:
    """
    Retrieves a logger with the specified name.
    
    Args:
        name (str): The name of the logger.
    
    Returns:
        logging.Logger: The configured logger.
    """
    return logging.getLogger(name)