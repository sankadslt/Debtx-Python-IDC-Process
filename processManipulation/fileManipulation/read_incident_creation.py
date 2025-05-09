#region - [Template Description]
''' 
    Purpose: This template is used for validating the incident creation CSV.
    Created Date: 2025-01-18
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2024-02-16
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)      
    Version: Python 3.12
    Dependencies:
    Related Files: file_operations.py, db_operations.py, loggers.py, custom_exception.py etc.
    Notes:  
    IP: The function processes an upload log entry and a task ID to handle Incident Creation. The upload log entry is a document containing:

        "file_upload_seq" (Unique identifier for the uploaded file)
        "File_Path" (Location of the file to process)
        "File_Type" (Type of the file)
        
    The function move and  processes a CSV file containing incident creation records with 4 values per row:Account number,DRS Action,Source-type, Monitor months.

    sample input(csv):518994139545,collect arrears,Pilot Suspended,3
                      2892945567,collect CPE,Product Terminate,2
                      5228103529,invalid,Special,3
                      
    OP:output appends additional messages from the system and  error messages if given values are empty or wrong

    sample output(csv):518994139545,collect arrears,Pilot Suspended,9|Accounts_Num must be 10 digits|Monitor Months cannot be greater than 3
                       2892945567,collect CPE,Product Terminate,2 ------------------->    calling the api for this record)
                       5228103529,invalid,Special,3|Invalid DRC Action
    
                       Error messages: Account_number must be 10 digits,Monitor Months cannot be greater than 3,Invalid DRS Action,Invalid Source Type
                        
                        when the valid records are found(records that are found formatted according to the validation criteria), call the Incident
                        Creation API . othewise Create and error file with the same name with .err extention and append the error message at the end of 
                        each record as shown in the sample output(csv) record 1 & 3 
                        
'''
#endregion


#region Importing Libraries
import os
import shutil
import time
import csv
import requests
from datetime import datetime
from utils.coreConfig import config
from utils.db import db
from utils.customExceptions.customExceptions import TaskProcessingError, ValidationError
from utils.logger.loggers import get_logger
from utils.Mongo_Shared_Processing.mongo_shared_processing import update_status_to_inprogress
#endregion

def read_incident_creation(file_upload_log_entry, task_id,file_upload_seq,file_type,file_path):
    """
    Process an 'Incident Creation' record_file by validating records and forwarding valid ones to an API.
    """
    logger = get_logger("incident_creation")
    
    file_upload_log_collection = db["file_upload_log"]
    system_tasks_collection = db["System_tasks_inprogress"]
    system_tasks_inprogress_collection= db["System_tasks_Inprogress"]
    
    # Update task statuses on System task,System Tasks inprogress & file_upload_log documents to "inProgress"
    update_status_to_inprogress(task_id, file_upload_seq, db, logger)                                          
    #endregion

    # Get the Created_By from corresponding system task document
    system_task_entry = system_tasks_collection.find_one({"Task_Id": task_id})
    if not system_task_entry or "Created_By" not in system_task_entry:
        logger.error(f"'created_by' field is missing in system task for task {task_id}. Cannot proceed.")
        raise TaskProcessingError(task_id, "'created_by' field missing in system_task", logger)

    created_by = system_task_entry["Created_By"]
    '''#contact_number = .get("Contact_Number") #get this from the oss/bss not system_task_entry'''

    while True:
        new_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv" 
        new_file_path = os.path.join(config.get("incident_creation_path"), new_file_name)
        if not os.path.exists(new_file_path):
            break
        time.sleep(1)

    os.makedirs(config.get("incident_creation_path"), exist_ok=True)
    # Get the original file name from upload log and build the full source path
    original_file_name = file_upload_log_entry.get("File_Name")
    source_file_path = os.path.join(file_path, original_file_name)

    # Check if the original file exists before proceeding
    if not os.path.exists(source_file_path):
        logger.error(f"Expected file '{original_file_name}' not found at {file_path}. Cannot proceed further.")
        raise TaskProcessingError(task_id, f"File not found: {source_file_path}", logger)
      

    try:
        # Move the file to the incident directory & update log entry with Forwarded_File_Path
        if os.path.exists(file_path):
            try:
                shutil.move(source_file_path, new_file_path)
                logger.info(f"File moved to {new_file_path}")
                
                file_upload_log_collection.update_one(
                    {"file_upload_seq": file_upload_seq},
                    {"$set": {"Forwarded_File_Path": new_file_path}},
                )
            except (OSError, IOError) as e:
                logger.error(f"Error moving record_file: {e}")
                raise TaskProcessingError(task_id, f"Error moving record_file: {e}", logger)
        else:
            logger.warning(f"File not found at {file_path}. Cannot proceed.")
            return

        # Initialize counters
        total_record_count, success_count, error_count = 0, 0, 0
        error_file_path = None
        err_writer = None
        err_file = None

        try:
            with open(new_file_path, "r", encoding="utf-8-sig") as record_file:
                file_reader = csv.reader(record_file)

                # Prepare error file if needed
                error_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.err.csv"
                error_file_path = os.path.join(config.get("incident_creation_path"), error_file_name)
                err_file = open(error_file_path, "w", newline="")
                err_writer = csv.writer(err_file)

                for row in file_reader:
                    try: 
                        total_record_count += 1
                        
                         # region Validation checks-begin
                        if len(row) < 4:
                            raise ValidationError(
                                         total_record_count, "Row", "Row does not have enough values. Expected 4."
                         )

                        account_num, drc_action, source_type, monitor_months =[col.strip() for col in row[:4]]
                        
                       
                        if not (account_num.isdigit() and len(account_num) == 10):
                            raise ValidationError(
                                total_record_count, "Account_Num", "Accounts_Num must be 10 digits."
                            )
                        #Validate DRC_Action    
                        match drc_action:
                           case "collect arrears" | "collect arrears and CPE" | "collect CPE":
                            pass  # Valid case, do nothing
                           case _:
                            raise ValidationError(
                                total_record_count, "DRC_Action", "Invalid DRC Action."
                            )

                        # Validate Source_Type using match
                        match source_type:
                            case "Pilot Suspended" | "Product Terminate" | "Special":
                                pass  # Valid case
                            case _:
                                raise ValidationError(
                                    total_record_count, "Source_Type", "Invalid Source Type."
                                )
                        #validate Monitor Months        
                        monitor_months = (
                            3 if monitor_months == "0" or not monitor_months.isdigit() else int(monitor_months)
                        )
                        if monitor_months > 3:
                            raise ValidationError(
                                total_record_count, "Monitor_Months", "Monitor Months cannot be greater than 3."
                            )
                        # endregion Validation checks-end
                        
                        #region API call to create incident
                        
                        if drc_action =="collect CPE":
                            contact_number = system_task_entry.get("Contact_Number") ##The contact number will be on OSS/BSS not system_task_entry
                            if not contact_number:
                                error_count +=1
                                raise ValidationError(total_record_count,"collect CPE" , "API Failed for the 'Collect-CPE' DRC Action.Missing or invalid number provided")
                            
                        
                        payload = {
                            "Account_Num": account_num,
                            "DRC_Action": drc_action,
                            "Monitor_Months": monitor_months,
                            "Created_By": created_by,
                            "Source_Type": source_type,
                        }      #if the DRC_Action is collect CPE then add cantact number to the payload(contact_number source=)
                        
                        if drc_action == "collect CPE":
                            payload["Contact_number"] = contact_number
                        

                        response = requests.post(config.get("incident_creation_endpoint"), json=payload, timeout=10)
                        logger.info(f"API request sent to {config.get("incident_creation_endpoint")} with payload: {payload}")
                        logger.info(f"API response received: {response.status_code}, {response.text}")
                        response.raise_for_status()  # Raises HTTPError for 4xx and 5xx responses

                        response_json = response.json()
                        success_count += 1
                        logger.info(f"Record {total_record_count} passed validation. API response: {response_json}")
                         #endregion API call to create incident

                    except ValidationError as e:
                        error_count += 1
                        err_writer.writerow(row + [f"|{e.message}"])
                        logger.error(str(e))
                    except requests.exceptions.ConnectionError:
                        logger.error(f"Failed to connect to the API for record {total_record_count}. Possible network issue.")
                    except requests.exceptions.Timeout:
                        logger.error(f"Request timed out for record {total_record_count}. API did not respond in time.")
                    except requests.exceptions.RequestException as e:
                        logger.error(f"API request failed for record {total_record_count}. Error: {e}")
                        
                       
                        
        except csv.Error as e:
            logger.error(f"CSV file read error: {e}")
            raise TaskProcessingError(task_id, f"CSV file read error: {e}", logger)
        finally:
            if err_file:
                err_file.close()

        #Formatting the Status descriptions for file_upload log and System task
        status_description = (
            "Incident creation completed successfully."
            if error_count == 0
            else "Incident creation completed with some validation errors. API called for valid records."
        )
        
        
        #region Update the "system task"& "file_upload_log" statuses = "Complete" with a description
        file_upload_log_collection.update_one(
            {"file_upload_seq": file_upload_seq},
            {"$set": {
                "total_record_count": total_record_count,
                "success_count": success_count,
                "error_count": error_count,
                "log_status": "Completed",
                "log_status_description": status_description,
                "error_file_path": error_file_path,
            }},
        )
        logger.info(f"File upload log {file_upload_seq} status set to 'Completed'.")
       
        system_tasks_collection.update_one(
            {"Task_Id": task_id},
            {"$set": {
                "task_status": "Completed",
                "task_status_description": status_description,
                "total_record_count": total_record_count,
                "success_count": success_count,
            }},
        )
        logger.info(f"Task {task_id}: Marked as Completed in system_tasks.")
        
        system_tasks_inprogress_collection.update_one(
            {"Task_Id": task_id},
            {"$set": {
                "task_status": "Completed",
                "task_status_description": status_description,
                "total_record_count": total_record_count,
                "success_count": success_count,
            }},
        )
        logger.info(f"File upload log {file_upload_seq} status set to 'Completed'.")

        logger.info(f"Task {task_id}: Marked as Completed.")
        #endregion
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        raise TaskProcessingError(task_id, f"Unexpected error occurred: {e}", logger)