#region-Template
''' 
    Purpose: This template is used for Calling the \Case_Distribution_To_DRC API for each case_id in the batch approved case distribution to DRC process.
    Created Date: 2025-03-26
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2025-04-01
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)      
    Version: Python 3.12
    Dependencies: 
    Related Files: Batch_Approved_Case_Process.py
    Notes:  
    process: This process starts with finding the system task with Temoplate_Task_Id 29 and task_status Open.,
             then it will get the case distribution batch id from parameters and find the corresponding approver from -
             Template_forwarded_approver collection.
             then it will check the approver type is DRC_Distribution and get the case distribution details from-
             Template_case_distribution_drc_details collection using ,
             case_distribution_batch_id.then for all the cases listed with that batch id program checks the proceed_on field is None or not.
             if it is null, it will call the API \Case_Distribution_To_DRC with case_id as parameter.
             onse the API is called, it will update the task status to Complete and task description with the number of errors occurred.
             
    Collections:
    -System_tasks:                             -Read
    -Temp_forwarded_approver :                 -Read
    -Temp_case_distribution_drc_details:       -Read

'''
#endregion

from actionManipulation.batch_approved_case_distribution_to_drc.Batch_Approved_Case_Process import Batch_Approved_Case_Distribution_To_DRC
from utils.custom_Exceptions.cust_exceptions import FileMissingError
from utils.logger.loggers import get_logger
from utils.config_loader import config

logger = get_logger("ActionControllerLogger")

def main():
    try:
        
        print("Configuration values : ", config.__dict__)
        logger.info("Configuration loaded successfully")
        logger.info("Starting Batch Approved Case Distribution To DRC process...")

        Batch_Approved_Case_Distribution_To_DRC()

        logger.info("Batch Approved Case Distribution To DRC process completed...")
        print("-" * 80)

    except Exception as e:
        logger.error(e)

if __name__ == "__main__":
    main()
    
    
    
