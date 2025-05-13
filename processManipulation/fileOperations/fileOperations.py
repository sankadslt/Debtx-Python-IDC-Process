# region=[Template Description]
'''
    Purpose: This template is used for getting 'Open' tasks from the database and directing them to relevant functions.
    Created Date: 2025-01-18
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2024-01-29
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Version: Python 3.12
    Dependencies:
    Related Files: db_operations.py, ConnectDB.py, loggers.py, custom_exception.py etc.
    Notes:
    The process and validate file function is contain the Open tasks and it will get the relevant file upload log
       entry and in that log entry get the file type and direct to the relevant function to process and validate the file.

       FILE Type                 Function

       Incident Creation -------> read_incident_creation
       Incident Reject ---------> read_incident_reject
       Validity Period Extend ---> read_validity_period_extend
       Hold --------------------> read_case_hold
       Discard -----------------> read_discard


    
        if the file type is not in the file_processing_functions the function will raise an error.
        
    IP: tasks     


'''
# endregion

# region-imports
from pymongo import MongoClient
from utils.coreConfig import config
from utils.db import db
from utils.logger.loggers import get_logger
from utils.customExceptions.customExceptions import TaskProcessingError
from processManipulation.fileManipulation.read_incident_creation import read_incident_creation
from processManipulation.fileManipulation.read_incident_reject import read_incident_reject
from processManipulation.fileManipulation.read_validity_period_extend import read_validity_period_extend
from processManipulation.fileManipulation.read_case_hold import read_case_hold
from processManipulation.fileManipulation.read_case_discard import read_case_discard

# endregion


logger = get_logger("file_operations")


def process_and_validate_file(task):
    """
    Process and validate the uploaded file based on its type.
    """
    task_id = task["Task_Id"]


    try:
        # Change file_upload_seq to open list of documents
        file_upload_seq = int(task["parameters"]["file_upload_seq"])  # Directly accessing since db_operation ensures it's valid
        logger.info(f"Fetching file_type for file_upload_seq: {file_upload_seq}")

        file_upload_log_collection = db["file_upload_log"]
        file_upload_log_entry = file_upload_log_collection.find_one({"file_upload_seq": file_upload_seq})
        if not file_upload_log_entry:
            raise TaskProcessingError(
                task_id=task_id,
                error_message=f"No matching file_upload_log entry found for file_upload_seq {file_upload_seq}.",
                logger=logger
            )

        file_type = file_upload_log_entry.get("File_Type")
        logger.info(f"File type retrieved: {file_type}")

        if not file_type:
            raise TaskProcessingError(
                task_id=task_id,
                error_message="file_type is missing in file_upload_log.",
                logger=logger
            )
            
        file_path = config.get("uploaded_file_saved_dir", "default_path")
        if not file_path:
            raise TaskProcessingError(
                task_id=task_id,
                error_message="File path is not configured in the settings.",
                logger=logger
            )    

        # Process file based on its type using match case
        match file_type:
            case "Incident Creation":
                 read_incident_creation(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path)
            case "Incident Reject":
                 read_incident_reject(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path)
            case "Validity Period Extend":
                 read_validity_period_extend(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path)
            case "Hold":
                read_case_hold(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path)
            case "Discard":
                 read_case_discard(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path)
            case _:
                raise TaskProcessingError(
                    task_id=task_id,
                    error_message=f"Unsupported file_type '{file_type}'.",
                    logger=logger
                )

    except TaskProcessingError as e:
        logger.error(f"Task {task_id} failed due to processing error: {e}")

    except Exception as e:
        logger.exception(f"Unexpected error processing task {task_id}: {e}")

    except Exception as e:
        logger.exception("Error connecting to MongoDB client.")


