# region [Template Description]
'''
    Purpose: This template is used for get 'Open' tasks from the databse and get the corresponding log entry.
    Created Date: 2025-01-18
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2024-02-20
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Version: Python 3.12
    Dependencies:
    Related Files: file_operations.py, ConnectDB.py, loggers.py, custom_exception.py etc.
    
    Program Description:
    1. The function fetches system tasks from the system_task_inprogress collection where:

          Template_Task_id = 1

          task_status = "Open"

    2.Once general validations are passed, 
        the function attempts to find the corresponding log entry from the file_upload_log collection using the file_upload_seq.

    3.If both the "Open" system task and the corresponding log entry are found:

    4.The function sends the list of tasks to File_operations.py.
       
    Mongo collection: System_tasks_Inprogress
                      file_upload_log   

'''
# endregion

# region-imports
from pymongo import MongoClient
from utils.logger.loggers import get_logger
from utils.customExceptions.customExceptions import TaskProcessingError
from processManipulation.fileOperations.fileOperations import process_and_validate_file
from utils.coreConfig import config
from utils.db import db
# endregion

logger = get_logger("db_operations")


def get_open_tasks():
    upload_task_number = config.get("upload_task_number")
    system_tasks_inprogress_collection = db["System_tasks_Inprogress"]
    
    
    logger.info(f"Cleaning up completed/failed tasks from System_tasks_Inprogress...")

    try:
        # Delete completed or failed tasks
        cleanup_result = system_tasks_inprogress_collection.delete_many({
            "task_status": {"$in": ["Completed", "Failed"]}
        })
        logger.info(f"Deleted {cleanup_result.deleted_count} completed/failed tasks from System_tasks_Inprogress.")
    except Exception as e:
        logger.exception("Failed to clean up System_tasks_Inprogress.")
  
    logger.info(f"Fetching open tasks with Template task ID {upload_task_number} && task_status = 'Open'...")


    try:
        collected_tasks = system_tasks_inprogress_collection.find(
            {"Template_Task_Id": {"$in":upload_task_number}, "task_status": "Open"}
            )
        logger.info(f"Start processing collected tasks...")
        
        for task in collected_tasks:
            task_id = task["Task_Id"]

            try:
                # Retrieve and convert file_upload_seq
                file_upload_seq = task.get("parameters", {}).get("file_upload_seq")
                if not file_upload_seq:
                    raise TaskProcessingError(task_id, "Missing file_upload_seq in task parameters.", logger)
                # Log raw value and convert to integer
                logger.info(f"Raw file_upload_seq from system_task: {file_upload_seq}")
                
                try:
                    file_upload_seq = int(file_upload_seq)
                    logger.info(f"Converted file_upload_seq '{file_upload_seq}' to integer: {file_upload_seq}")
                except ValueError:
                    raise TaskProcessingError(task_id, f"Invalid file_upload_seq format: {file_upload_seq}. Must be an integer.", logger)

                # Query file_upload_log
                logger.info(f"Querying file_upload_log with file_upload_seq: {file_upload_seq}")
                
                file_upload_log_collection = db["file_upload_log"]
                
                file_uploadlog_entry = file_upload_log_collection.find_one(
                    {"file_upload_seq": file_upload_seq,
                     "log_status": "Open"
                     }
                    )
                if not file_uploadlog_entry:
                    raise TaskProcessingError(task_id, f"No matching log found for file_upload_seq {file_upload_seq}.", logger)

                logger.info(f"Found matching log entry: {file_uploadlog_entry}")
                
                process_and_validate_file(task)

            except TaskProcessingError as e:
                logger.info(f"Task {task_id} skipped due to processing error: {e}")
              
            except Exception as e:
                logger.exception(f"Unexpected error processing task {task_id}: {e}")
                
    except Exception as e:
            logger.exception("Error fetching open tasks.")
            return []  # Return an empty list if there's an error fetching tasks
        
       
 