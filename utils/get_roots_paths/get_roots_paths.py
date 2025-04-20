from pathlib import Path

def get_project_root():
    """
    Returns the project root directory dynamically.
    Assumes this script is inside the project directory.
    """
    return Path(__file__).resolve().parent.parent.parent 

def get_config_filePath():    
    """
    Returns the full path to a file located in the Config directory.
    
    :param file_name: Name of the file (without extension) to retrieve.
    :return: Full path to the specified file in the Config directory.
    """
    project_root = get_project_root()
    print(f"Project root: {project_root}")
    config_file_path = project_root / "Config" /"drs_core_config.ini"
    return config_file_path

def get_logger_filePath():    
    """
    Returns the full path to a file located in the Config directory.
    
    :param file_name: Name of the file (without extension) to retrieve.
    :return: Full path to the specified file in the Config directory.
    """
    project_root = get_project_root()
    config_file_path = project_root / "Config" /"logConfig.ini"
    return config_file_path