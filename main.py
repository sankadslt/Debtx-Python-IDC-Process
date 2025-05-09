# region - [Template Description]
'''
    Purpose: This template is for Runing the script.
    Created Date: 2025-01-18
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2024-02-16
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Version: Python 3.12
    Dependencies:
    Related Files: file_operations.py, db_operations.py, loggers.py, custom_exception.py etc.
    Notes:  This file is used for running the entire program script
    IP:
    OP:
'''
# endregion

from utils.logger.loggers import get_logger
from utils.coreConfig import config
from processManipulation.fileOperations.fileOperations import process_and_validate_file
from processManipulation.databaseOperations.databaseOperations import get_open_tasks
from utils.customExceptions.customExceptions import TaskProcessingError

logger = get_logger("main")


def main():


        try:
            tasks = get_open_tasks()
            logger.info(f"Number of open tasks fetched: {len(tasks)}")
        except Exception as e:
            logger.exception("Failed to fetch open tasks.")
            return

        for task in tasks:
            task_id = task["_id"]
            logger.debug(f"Processing task: {task_id}")

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



if __name__ == "__main__":
    logger.info("Starting the script...")
    main()
    logger.info("Script execution completed.")