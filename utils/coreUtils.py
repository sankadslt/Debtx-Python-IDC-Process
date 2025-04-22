from pathlib import Path
import configparser

# Public variable to store configuration values
config_values = {}

def load_config():
    # ------------------ READ CONFIG ------------------
    config = configparser.ConfigParser()

    # Get the current file path
    current_file = Path(__file__).resolve()

    # Locate the project root
    project_root = current_file.parents[1]
    print("Project root: ", project_root)

    # Construct the path to the coreConfig.ini file
    core_config_file_path = project_root / "Config" / "coreConfig.ini"
    print("Configuration file path: ", core_config_file_path)

    if not core_config_file_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {core_config_file_path}")

    # Read the coreConfig.ini file
    config.read(core_config_file_path)



    # Get the environment value
    try:
        extracted_environment = config.get("ENVIRONMENT", "DATABASE")
        config_values["environment"] = extracted_environment
    except configparser.NoSectionError:
        raise Exception("Missing ENVIRONMENT section in configuration file.")
    except configparser.NoOptionError:
        raise Exception("Missing 'DATABASE' option in ENVIRONMENT section.")


    try:
        mongo_uri_with_db_name = config.get("MONGODB", extracted_environment)
        # Check if MongoDB URI is not empty or null
        if not mongo_uri_with_db_name.strip():
            raise ValueError(f"{extracted_environment} MongoDB URI is empty or null.")
    except configparser.NoSectionError:
        raise Exception("Missing mongoDB section in configuration file.")
    except configparser.NoOptionError:
        raise KeyError(f"No mongoDB URI found for {extracted_environment}.")

    # Separate database name and mongo uri (by the last '/')
    extracted_mongo_uri, extracted_database_name = mongo_uri_with_db_name.rsplit("/", 1)
    config_values["mongo_uri"] = extracted_mongo_uri
    config_values["database_name"] = extracted_database_name


#--------------------------------------------API ENDPOINTS--------------------------------------------
    # Check the API_ENDPOINTS section
    try:
        extracted_case_distribution_to_drc_endpoint = config.get("API_ENDPOINTS", "Case_Distribution_To_DRC")
        if not extracted_case_distribution_to_drc_endpoint.strip():
            raise ValueError("case distribution to drc endpoint is empty or null.")
        config_values["case_distribution_to_drc_endpoint"] = extracted_case_distribution_to_drc_endpoint
    except configparser.NoSectionError:
        raise Exception("Missing API endpoints in configuration file.")
    except configparser.NoOptionError:
        raise KeyError("Missing 'GET_CASE_PHASE' option.")
    except ValueError as e:
        raise Exception(f"Invalid value for 'GET_CASE_PHASE' endpoint.")
    

    # ------------------ TASK ID ------------------
    try:
        template_task_id = config.get("BatchApprovedCaseDistributedToDRCTemplate", "Template_Task_Id")
        if not template_task_id.strip():
            raise ValueError("Template_Task_Id is empty or null.")
        config_values["template_task_id"] = int(template_task_id)
    except configparser.NoSectionError:
        raise Exception("Missing BatchApprovedCaseDistributedToDRCTemplate section in configuration file.")
    except configparser.NoOptionError:
        raise KeyError("Missing 'Template_Task_Id' option.")
    except ValueError:
        raise Exception("Template_Task_Id should be an integer.")    


    # Return the config_values global hash map
    return config_values