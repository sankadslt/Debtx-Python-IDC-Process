'''
read_template_task_id_ini.py file is as follows:

    Purpose: This script reads the TEMPLATE_TASK_ID from an INI file.
    Created Date: 2025-03-01
    Created By:  T.S.Balasooriya (tharindutsb@gmail.com) , Pasan(pasanbathiya246@gmail.com),Amupama(anupamamaheepala999@gmail.com)
    Last Modified Date: 2025-03-15
    Modified By: T.S.Balasooriya (tharindutsb@gmail.com), Pasan(pasanbathiya246@gmail.com),Amupama(anupamamaheepala999@gmail.com)     
    Version: Python 3.9
    Dependencies: configparser, utils.loggers, utils.Custom_Exceptions
    Related Files: Case_controller.js
    Notes:
'''

import configparser
from utils.loggers import get_logger
from utils.Custom_Exceptions import INIFileReadError
from utils.get_root_paths import get_config_filePath


logger = get_logger("tmp_ini_reader")


def get_ini_value(section: str, key: str, default_value: str = None) -> str:
    """
    Generic function to read a value from an INI file.
    Args:
        section (str): The section in the INI file.
        key (str): The key within the section.
        default_value (str, optional): Default value to return if key not found.
    Returns:
        str: The value corresponding to the section and key, or default_value.
    Raises:
        INIFileReadError: If file reading fails and no default provided.
    """
    file_path = None
    try:
        config = configparser.ConfigParser()
        file_path = get_config_filePath()
        config.read(file_path)

        if section in config and key in config[section]:
            value = config[section][key]
            logger.info(f"Read {key} from section [{section}] in {file_path}: {value}")
            return value
        elif default_value is not None:
            logger.warning(f"Using default value for {key} in co [{section}]")
            return default_value
        else:
            error_message = f"{key} not found in section [{section}] of {file_path}"
            logger.error(error_message)
            raise INIFileReadError(error_message)
    except FileNotFoundError as fnf_error:
        if default_value is not None:
            logger.warning(f"Config file not found, using default value: {fnf_error}")
            return default_value
        logger.error(f"Config file not found: {fnf_error}")
        raise INIFileReadError(f"Config file not found: {fnf_error}")
    except Exception as error:
        logger.error(f"Failed to read INI file {file_path or 'Unknown'}: {error}")
        raise INIFileReadError(f"Failed to read INI file {file_path or 'Unknown'}: {error}")
    
def get_template_task_id(default_value: int = None) -> int:
    """
    Gets the Template_Task_Id from the INI file.
    Args:
        default_value (int, optional): Default value to return if not found.
    Returns:
        int: The Template_Task_Id value.
    Raises:
        INIFileReadError: If the value is not found or not an integer and no default provided.
    """
    try:
        value = get_ini_value("TEMPLATE_TASK", "Template_Task_Id", str(default_value) if default_value is not None else None)
        return int(value)
    except ValueError:
        if default_value is not None:
            logger.warning(f"Template_Task_Id must be an integer. Using default value: {default_value}")
            return default_value
        raise INIFileReadError(f"Template_Task_Id must be an integer. Got: {value}")