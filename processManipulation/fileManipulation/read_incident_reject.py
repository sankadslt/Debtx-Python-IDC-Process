#region-Template
''' 
    Purpose: This template is used for validating the incident creation CSV.
    Created Date: 2025-02-24
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2025-03-05
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)      
    Version: Python 3.12
    Dependencies: 
    Related Files: file_operations.py, db_operations.py, loggers.py, custom_exception.py etc.
    Notes:  
    IP: The function processes an upload log entry and a task ID to handle incident rejections. The upload log entry is a document containing:

        "file_upload_seq" (Unique identifier for the uploaded file)
        "File_Path" (Location of the file to process)
        "File_Type" (Type of the file)
        
    The function move and  processes a CSV file containing incident rejection records with 2 values per row:Account number & Rejected reason.
    
    sample input(csv):12345678901,Invalid document
                      0987654321,Missing information
                      4532153134,
      
    OP:output appends additional messages from the system and  error messages if given values are empty or wrong
    
    sample output(csv): 12345678901,Invalid document |Rejected by System due to invalid data|Invalid or missing account number
                        0987654321,Missing information |Rejected by System due to invalid data
                        4532153134,|Rejected by System due to invalid data|Reject reason is missing
                        
                        General append message: |Rejected by System due to invalid data
                        Error messages: Invalid or missing account number   , Reject reason is missing
                        
                        when the valid records are found(records that contains valid account number and reject reason) the system will update the incident status 
                        to "Incident Reject" and the status_description with the reject reason
'''
#endregion


#region-imports
import os
import shutil
import csv
from pymongo import UpdateOne
from datetime import datetime
from utils.logger.loggers import get_logger
from utils.customExceptions.customExceptions import TaskProcessingError , ValidationError
from utils.coreConfig import config
from utils.db import db
import time
from utils.Mongo_Shared_Processing.mongo_shared_processing import update_status_to_inprogress
#endregion

def read_incident_reject(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path):
    logger = get_logger("Incident Reject")
    
    file_upload_log_collection = db["file_upload_log"]
    system_tasks_collection = db["System_tasks"]
    system_tasks_inprogress_collection= db["System_tasks_Inprogress"]
    incident_collection = db["Incident"]
    
    
    #set the file upload log status to inprogress
    update_status_to_inprogress(task_id, file_upload_seq, db, logger)   
   
    while True:
        new_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv" 
        new_file_path = os.path.join(config.get("incident_reject_path"), new_file_name)
        if not os.path.exists(new_file_path):
            break
        time.sleep(1)

    os.makedirs(config.get("incident_reject_path"), exist_ok=True)
    # Get the original file name from upload log and build the full source path
    original_file_name = file_upload_log_entry.get("File_Name")
    source_file_path = os.path.join(file_path, original_file_name)

    # Check if the original file exists before proceeding
    if not os.path.exists(source_file_path):
        logger.error(f"Expected file '{original_file_name}' not found at {file_path}. Cannot proceed further.")
        raise TaskProcessingError(task_id, f"File not found: {source_file_path}", logger)

    try:
        #move the file and update file upload log with forwarded file path
        shutil.move(source_file_path, new_file_path)
        logger.info(f"File moved to {new_file_path}")

        file_upload_log_collection.update_one(
            {"file_upload_seq": file_upload_seq},
            {"$set": {"Forwarded_File_Path": new_file_path}}
        )
        
        #before validating set the total record count , error count to 0 , defined the error file path
        total_record_count, error_record_count = 0, 0
        error_file_path = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.err.csv"
        error_file_path = os.path.join(config.get("incident_reject_path"), error_file_path)

        with open(new_file_path, "r", encoding="utf-8-sig") as record_file, \
             open(error_file_path, "w", newline="", encoding="utf-8-sig") as err_file:

            file_reader = csv.reader(record_file)
            err_writer = csv.writer(err_file)

            for row in file_reader:
                if not row:
                    continue
                total_record_count += 1
                logger.info(f"Processing row: {row}")

                try:
                    match row:
                        case [account_num, rejected_reason] if account_num.strip() and rejected_reason.strip():
                            account_num = account_num.strip()
                            rejected_reason = rejected_reason.strip()

                            match len(account_num):
                                case 10:
                                    pass  # Valid account number
                                case _:
                                    raise ValidationError(total_record_count, "Account_Num", "Invalid or missing account number")
                            
                            
                            incident_collection_document_entry = incident_collection.find_one({"Account_Num": account_num})

                            match incident_collection_document_entry:
                                case None:
                                    raise ValidationError(total_record_count, "Account_Num", "No incident document found for this Account Number")
                                case _ if incident_collection_document_entry.get("Proceed_dtm") is None:
                                    incident_collection.update_one(
                                        {"Account_Num": account_num},
                                        {"$set": {
                                            "Incident_Status": "Incident Reject",
                                            "Incident_Status_Dtm": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                            "Status_Description": rejected_reason,
                                        }}
                                    )
                                    logger.info(f"Updated incident status to 'Incident Reject' for Account_Num: {account_num}")
                                case _:
                                    raise ValidationError(total_record_count, "Case_Status", "Case is created. Cannot reject")
                        
                        case [_, ""] | ["", _]:
                            raise ValidationError(total_record_count, "Row Format", "Missing required values in row")
                        
                        case _:
                            raise ValidationError(total_record_count, "Row Format", "Unexpected format in row")

                except ValidationError as e:
                    row.append(f"| {e.message}")
                    logger.error(f"Validation failed for row: {row}")
                    err_writer.writerow(row)
                    error_record_count += 1
                    continue  # Skip to the next row

        logger.info(f"Task {task_id}: Incident reject process completed.")
        
        #update the fileupload log and system task to completed with error count and total count
        file_upload_log_collection.update_one(
            {"file_upload_seq": file_upload_seq},
            {"$set": {
                "log_status": "Completed",
                "log_status_description": "Incident reject process completed",
                "total_record_count": total_record_count,
                "error_record_count": error_record_count,
            }}
        )
        logger.info(f"File upload log updated with status 'Completed' for file upload seq: {file_upload_seq}")
        #update the system task document to completed
        system_tasks_collection.update_one(
            {"Task_Id": task_id},
            {"$set": {
                "task_status": "Completed",
                "task_status_description": "Incident reject process completed",
                "total_record_count": total_record_count,
                "error_record_count": error_record_count,
            }}
        )
        logger.info(f"System task updated with status 'Completed' for task ID: {task_id}")
        system_tasks_inprogress_collection.update_one(
            {"Task_Id": task_id},
            {"$set": {
                "task_status": "Completed",
                "task_status_description": "Incident reject process completed",
                "total_record_count": total_record_count,
                "error_record_count": error_record_count,
            }}
        )
        logger.info(f"System tasks in progress updated with status 'Completed' for task ID: {task_id}")

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}", exc_info=True)
        raise TaskProcessingError(task_id, f"Unexpected error: {e}", logger)




