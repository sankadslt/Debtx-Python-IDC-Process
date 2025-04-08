import configparser
from utils.logger.loggers import get_logger
from utils.custom_Exceptions.cust_exceptions import INIFileReadError
from utils.get_roots_paths.get_roots_paths import get_config_filePath

logger = get_logger("ini_reader")

def get_ini_value(section: str, key: str) -> str:
    """
    Generic function to read a value from an INI file.
    Args:
        section (str): The section in the INI file.
        key (str): The key within the section.
    Returns:
        str: The value corresponding to the section and key.
    Raises:
        INIFileReadError: If file reading or value extraction fails.
    """
    try:
        config = configparser.ConfigParser()
        file_path = get_config_filePath()
        config.read(file_path)

        if section in config and key in config[section]:
            value = config[section][key]
            logger.info(f"Read {key} from section [{section}] in {file_path}: {value}")
            return value
        else:
            error_message = f"{key} not found in section [{section}] of {file_path}"
            logger.error(error_message)
            raise INIFileReadError(error_message)
    except Exception as error:
        logger.error(f"Failed to read INI file {file_path}: {error}")
        raise INIFileReadError(f"Failed to read INI file {file_path}: {error}")

def get_template_task_id() -> int:
    """
    Gets the Template_Task_Id from the INI file.
    Returns:
        int: The Template_Task_Id value.
    Raises:
        INIFileReadError: If the value is not found or not an integer.
    """
    value = get_ini_value("BatchApprovedCaseDistributedToDRCTemplate", "Template_Task_Id")
    try:
        return int(value)
    except ValueError:
        raise INIFileReadError(f"Template_Task_Id must be an integer. Got: {value}")
