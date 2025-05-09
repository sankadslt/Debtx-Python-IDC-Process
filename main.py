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
from utils.task_runner import main


logger = get_logger("main")





if __name__ == "__main__":
    logger.info("Starting the script...")
    main()
    logger.info("Script execution completed.")