from utils.logger.loggers import get_logger
from processManipulation.fileOperations.fileOperations import process_and_validate_file
from processManipulation.databaseOperations.databaseOperations import get_open_tasks
from utils.customExceptions.customExceptions import TaskProcessingError
from utils.coreConfig import config

logger = get_logger("task_runner")


def task_runner():
    try:
        tasks = get_open_tasks()
        if not tasks:
            logger.warning("No open tasks to process or failed to fetch tasks.")
            return
        logger.info(f"Number of open tasks fetched: {len(tasks)}")
    except Exception as e:
        logger.exception("Failed to fetch open tasks.")
        return

    for task in tasks:
        task_id = task["Task_Id"]
        file_upload_seq = task.get("parameters", {}).get("file_upload_seq")
        
        if not file_upload_seq:
            logger.warning(f"Skipping task {task_id} due to missing file_upload_seq.")
            continue

        try:
            logger.info(f"Processing task {task_id} with file_upload_seq: {file_upload_seq}")
            process_and_validate_file(task)
        except TaskProcessingError as e:
            logger.error(f"Task {task_id} encountered a processing error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error processing task {task_id}: {e}")
