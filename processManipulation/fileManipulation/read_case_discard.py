#region-Template
''' 
    Purpose: This template is used for validating and processing case hold CSV document,and updating the case_details document.
    Created Date: 2025-03-17
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2025-03-17
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)      
    Version: Python 3.12
    Dependencies: 
    Related Files: file_operations.py, db_operations.py, loggers.py, custom_exception.py etc.
    Notes:  
    IP:The function processes an upload log entry and a task ID to handle case Discards. the upload log entry is containing:
    
        "file_upload_seq" (Unique identifier for the uploaded file)
        "File_Path" (Location of the file to process)
        "File_Type" (Type of the file)
        
    The function move and processes the CSV files containing case_discard records with not more than values per row:
                  
                  Telephone number      or 
                  case_id               or
                  Account number 
                  & 
                  Hold reason.    
                  
        Example input(csv):12345,1234567890,011xxxxxxx,Document verification pending
                                ,1242345646,          ,missing payement details
                           56372,          ,011xxxxxxx,Complience issue 
                           001, , , 
                                
    
    
    OP:output appends additional messages from the system ,  error messages if given values are empty or wrong to the CSV and update the reletive case_details document.
    
    
        Example output(csv): 12345,1234567890,011xxxxxxx,Document verification pending ----->case id 12345 updated to the status Discard
                             123455637,          ,011xxxxxxx,Complience issue ------>updated
                                  ,1242345646,          ,missing payement details -----> At least one of Valid Case ID, Account Number, or Telephone Number must be present
                             001,    -----------------> |Discard Reason is missing (in .err file)    
                                 
            
            Error messages: Invalid account number,invalid case_id,invalid telephone_number(if 3 of them missing or failed the validations), Discard reason is missing,
                            No matching case found for given details- when two or more values are passed the validation but Failed to find the matching case in the case_details   
                        
                        
        when the valid records are found(records that contains valid account number or case_id or Telephone number and discard reason) the system will update the matching case document status 
        to "Discard" and the status_description with the Discard reason in the case_details document.                
'''
#endregion

#region-imports
import os
import shutil
import time
import csv
from datetime import datetime
from utils.customExceptions.customExceptions import TaskProcessingError, ValidationError
from utils.query.build_case_query import build_case_query
from utils.logger.loggers import get_logger
from utils.coreConfig import config
from utils.db import db
from utils.Mongo_Shared_Processing.mongo_shared_processing import update_status_to_inprogress
#endregion  

def read_case_discard(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path):
    logger = get_logger("Case Discard")
    
    #get the file path and update the file upload log to inprogress
    file_upload_log_collection = db["file_upload_log"]
    system_tasks_collection = db["System_tasks"]
    system_tasks_inprogress_collection= db["System_tasks_Inprogress"]
    case_details = db["Case_details"]


    
    update_status_to_inprogress(task_id, file_upload_seq, db, logger)   

    while True:
        new_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv" 
        new_file_path = os.path.join(config.get("case_discard_path"), new_file_name)
        if not os.path.exists(new_file_path):
            break
        time.sleep(1)

    os.makedirs(config.get("case_discard_path"), exist_ok=True)

    # Get the original file name from upload log and build the full source path
    original_file_name = file_upload_log_entry.get("File_Name")
    source_file_path = os.path.join(file_path, original_file_name)

    # Check if the original file exists before proceeding
    if not os.path.exists(source_file_path):
        logger.error(f"Expected file '{original_file_name}' not found at {file_path}. Cannot proceed further.")
        raise TaskProcessingError(task_id, f"File not found: {source_file_path}", logger)
    
    try:
        #move the file and uodate the file path with new file path
        shutil.move(source_file_path, new_file_path)
        logger.info(f"File moved to {new_file_path}")

        file_upload_log_collection.update_one(
            {"file_upload_seq": file_upload_seq},
            {"$set": {"Forwarded_File_Path": new_file_path}}
        )
        
        #set total count , error count to 0 
        total_record_count, total_error_count = 0, 0
        error_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.err.csv" 
        error_file_path = os.path.join(config.get("case_discard_path"), error_file_name)
        
        with open(new_file_path, "r", encoding="utf-8-sig") as record_file, \
             open(error_file_path, "w", newline="", encoding="utf-8-sig") as err_file:
            file_reader = csv.reader(record_file)
            err_writer = csv.writer(err_file)

            for row in file_reader:
                if not row:
                    continue

                total_record_count += 1
                logger.info(f"Processing row {total_record_count}: {row}")

                try:
                    #Row validation begin
                    if len(row) < 4:
                        raise ValidationError(total_record_count, "Row Format", "Missing values in row")
                    
                    case_id, account_number, telephone_number, discard_reason = map(str.strip, row[:4])
                    
                    #validate the row values
                    match (case_id, account_number, telephone_number, discard_reason):
                        case (_, _, _, ""):
                            raise ValidationError(total_record_count, "Discard Reason", "|Discard reason is missing")

                        case (case_id, _, _, _) if case_id and not case_id.isdigit():
                            raise ValidationError(total_record_count, "Case ID", "|Invalid case ID")

                        case (_, account_number, _, _) if account_number and (not account_number.isdigit() or len(account_number) != 10):
                            raise ValidationError(total_record_count, "Account Number", "|Invalid account number")

                        case (_, _, telephone_number, _) if telephone_number and (not telephone_number.isdigit() or not (9 <= len(telephone_number) <= 12)):
                            raise ValidationError(total_record_count, "Telephone Number", "|Invalid telephone number")
                    
                    #validated rows will be sent to the build case query
                    query = build_case_query(case_id, account_number, telephone_number)
                    case_document = case_details.find_one(query) if query else None

                    if not case_document:
                        raise ValidationError(total_record_count, "Case Lookup", "|No matching case found for given details")

                    #update the case details document status
                    case_details.update_one(
                        {"_id": case_document["_id"]},
                        {"$set": {
                            "case_current_status": "Discard",
                            "case_status.0.case_status": "Discard",
                            "case_status.0.status_reason": discard_reason
                        }}
                    )
                    logger.info(f"Updated case to Discarded status with hold reason: {discard_reason}")

                except ValidationError as e:
                    logger.error(f"Validation failed for row {total_record_count}: {e.message}")
                    err_writer.writerow(row + [e.message])
                    total_error_count += 1
                    continue
                
    except Exception as e:
        logger.error(f"Error reading file: {e}", exc_info=True)
        raise TaskProcessingError(task_id, f"Error reading file: {e}", logger)

    logger.info(f"Task {task_id}: Case discard process completed.")
    
    #update file upload log and system task document statusus to inprogress
    file_upload_log_collection.update_one(
        {"file_upload_seq": file_upload_seq},
        {"$set": {
            "log_status": "Completed",
            "log_status_description": "Case discard process completed",
            "total_record_count": total_record_count,
            "total_error_count": total_error_count  
        }}
    )
    logger.info(f"File upload log updated for file_upload_seq {file_upload_seq}")
    #update system task document status to completed
    system_tasks_collection.update_one(
        {"Task_Id": task_id},
        {"$set": {
            "task_status": "Completed",
            "task_status_description": "Case discard process completed",
            "total_record_count": total_record_count,
            "total_error_count": total_error_count  
        }}
    )
    logger.info(f"System task document updated for task_id {task_id}")
    #update system task inprogress document status to inprogress
    system_tasks_inprogress_collection.update_one( 
        {"Task_Id": task_id},
        {"$set": {
            "task_status": "Completed",
            "task_status_description": "Case discard process completed",
            "total_record_count": total_record_count,
            "total_error_count": total_error_count  
        }}
    )
    logger.info(f"System task inprogress document updated for task_id {task_id}")
