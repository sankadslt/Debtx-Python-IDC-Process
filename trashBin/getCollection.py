import configparser

# Import a custom logger for logging databaseOperations-related messages.
from utils.logger.loggers import get_logger
from utils.filePath.filePath import get_filePath

logger = get_logger("Database")


def get_collection_name(collection_key):
    """
    Retrieves the collection name from a configuration file based on the provided key.

    Parameters:
        collection_key (str): The key to look up in the configuration file under the [COLLECTIONS] section.

    Returns:
        str: The collection name if found; otherwise, the collection_key itself is returned.
    """

    try:
        # get file path for relevant os type
        config_path = get_filePath("DBConfig")
        # config_path = os.path.join(os.path.dirname(__file__), "D:\\DRS_Process\\DRS_Python_Backend\\Config\\databaseConfig.ini")

        # Create a ConfigParser object to read the INI file.
        config = configparser.ConfigParser()

        # Read the configuration file.
        config.read(config_path)
        read_files = config.read(config_path)

        # If no files were successfully read, raise a FileNotFoundError.
        if not read_files:
            raise FileNotFoundError

        return config['COLLECTIONS'].get(collection_key, collection_key)

    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")

    except Exception as e:
        logger.error(e)


