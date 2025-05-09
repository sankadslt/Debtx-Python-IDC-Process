# region-Template
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
    -tmp_forwarded_approver :                  -Read
    -Temp_Case_Distribution_DRC_Details:       -Read

'''
# endregion

from utils.custom_Exceptions.cust_exceptions import *
from logging import getLogger
import requests
from pymongo import MongoClient
from utils.config_loader import config
from utils.db import db
import requests
import json
import traceback



logger = getLogger()

def Batch_Approved_Case_Distribution_To_DRC():
    """
    Process system tasks for approved case distribution to DRC.
    """
    logger.info("Starting Batch_Approved_Case_Distribution_To_DRC process")
    
 
    
    total_error_count = 0  # Initialize the total error count

    try:
        logger.info("Connected to MongoDB database successfully")
        
        
        template_task_id = config.template_task_id     
         
        system_tasks = db.System_tasks.find({"Template_Task_Id": template_task_id, "task_status": "Open"})
        task_list = list(system_tasks)  # Convert cursor to a list
        logger.info(f"Found {len(task_list)} tasks to process.")

        for task in task_list:
            try:
                batch_id = task.get("parameters", {}).get("case_distribution_batch_id")
                if not batch_id:
                    raise ValidationError("Missing case_distribution_batch_id in task parameters")

                logger.info(f"Processing task {task['_id']} with batch_id {batch_id}")
                db.System_tasks.update_one({"_id": task["_id"]}, {"$set": {"task_status": "InProgress"}})
                
                #fetching the corresponding approver from Template_forwarded_approver collection
                approver = db.tmp_forwarded_approver.find_one({"approver_reference": batch_id})
                if not approver:
                    raise NotFoundError(f"Approver not found for batch_id: {batch_id}")
                
                #check if the approver type is DRC_Distribution
                if approver.get("approver_type") != "DRC_Distribution":
                    logger.error(f"Skipping task {task['_id']}, approver type is {approver.get('approver_type')}")
                    continue

                logger.info(f"Approver type DRC_Distribution found for batch_id {batch_id}")
               
                #fetching the case distribution details from Template_case_distribution_drc_details collection using case_distribution_batch_id
                case_distribution_drc_details = list(db.Tmp_Case_Distribution_DRC_Details.find({"case_distribution_batch_id": batch_id}))
                if not case_distribution_drc_details:
                    raise DataFetchError(f"No case distribution details found for batch_id: {batch_id}")

                batch_error_count = 0  # Track errors for this batch
                
                # Loop through each case distiribution DRC detail entry
                for case_drc_details in case_distribution_drc_details:
                    try:
                        # Extract case_id from the current record
                        case_id = case_drc_details.get("case_id", None)
                        # Skip processing if 'proceed_on' is already set
                        if case_drc_details.get("proceed_on") is not None:
                            logger.info(f"Skipping case_id {case_id} as 'proceed_on' is not None.")
                            continue
                        
                        if not case_id:
                            raise CaseIdNotFoundError("Missing case_id in case distribution details")
                        
                        # Construct the API URL using the case_id 
                        api_url = config.case_distribution_to_drc_endpoint + str(case_id)
                        

                        # Optional: Format the URL only if it has a placeholder for case_id
                        if "{case_id}" in api_url:
                             api_url = api_url.format(case_id=case_id)
                       
                        
                        logger.info(f"Calling API: {api_url} for case_id {case_id}")

                        response = requests.post(api_url)
                        response.raise_for_status() #raises and error for bad responses
                       
                        response_data = response.json()
                        logger.info(f"API response for case_id {case_id}: {json.dumps(response_data)}")

                    except requests.RequestException as api_error:
                        logger.error(f"API call failed for case_id {case_id}: {str(api_error)}")
                        batch_error_count += 1  # Continue processing even if API fails
                        continue
                
                #set task status to complete and update task description with error count
                db.System_tasks.update_one({"_id": task["_id"]}, {"$set": {
                    "task_status": "Complete",
                    "task_description": f"Task completed with {batch_error_count} errors"
                }})
                total_error_count += batch_error_count

                logger.info(f"Task {task['_id']} completed with {batch_error_count} errors.")
                
                logger.info(f"Batch process completed. Total tasks processed: {len(task_list)}, total errors: {total_error_count}")


            except CustomException as task_error:
                logger.error(f"Custom exception processing task {task['_id']}: {str(task_error)}")
            except Exception as task_error:
                db.System_tasks.update_one({"_id":task["_id"]},{"$set":{"task_status":"Failed"}})
                logger.error(f"Unexpected error processing task {task['_id']}: {str(task_error)}\n{traceback.format_exc()}")

        if total_error_count > 0:
            logger.error(f"Total errors encountered across all tasks: {total_error_count}")

    except CustomException as e:
        logger.error(f"Custom exception: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}\n{traceback.format_exc()}")

