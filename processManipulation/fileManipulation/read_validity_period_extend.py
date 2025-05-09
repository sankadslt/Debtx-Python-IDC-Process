# region-Template
'''
    Purpose: This template is used for validating the period extension CSV.
    Created Date: 2025-03-05
    Created By: Sandani Gunawardhana(sandanigunawardhana@gmail.com)
    Last Modified Date: 2024--
    Modified By: Sandani Gunawardhana(sandanigunawardhana@gmail.com)
    Version: Python 3.12
    Dependencies:
    Related Files: file_operations.py, db_operations.py, loggers.py, custom_exception.py etc.
    Notes:
    IP: The function processes an upload log entry of a task that need to handle validity period extension. The upload log entry is a document containing:

        "file_upload_seq" (Unique identifier for the uploaded file)
        "File_Path" (Location of the file to process)
        "File_Type" (Type of the file)

    The function move and  processes a CSV file containing validity period extension records with 4 values per row:Case ID, Account No, Telephone No, & Period in months.

    sample input(csv):12345,12345678901,0112177442,2
                      ,,,2
                      12345,,0112177442,2
                      ,,0112177442,2

    OP: If the process is successful:
            1. The monitor_months field in case_details is updated.
            2. The expire_dtm field in dtm[0] is extended accordingly.
            3. The task log and system task status are updated to "Completed.

        If errors occur
            1. An error file is generated containing rows with validation failures and associated error messages

                sample output(csv): ,,,2 |All Case ID, Account No, and Telephone No missing
                                    12345,12345678901,0112177442, |No of months missing
'''
# endregion


# region-imports
import os
import shutil
import csv
import requests

from datetime import datetime

from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import time

from utils.logger.loggers import get_logger
from utils.customExceptions.customExceptions import DataProcessingException, TaskProcessingError, \
    MissingIdentifiersException, MissingNoOfMonthsException, CaseNotFoundException, InvalidCaseStatusException, \
    PeriodExtensionException, FileDownloadError, CasePhaseException
from utils.Mongo_Shared_Processing.mongo_shared_processing import update_status_to_inprogress    
from utils.coreConfig import config
from utils.db import db
# endregion

logger = get_logger("Read Validity Period Extend")

def read_validity_period_extend(file_upload_log_entry, task_id, file_upload_seq, file_type,file_path):
    
    file_upload_log_collection = db["file_upload_log"]
    system_tasks_collection = db["System_tasks_inprogress"]
    system_tasks_inprogress_collection= db["System_tasks_Inprogress"]
    
    # Update task statuses on System task,System Tasks inprogress & file_upload_log documents to "inProgress"
    update_status_to_inprogress(task_id, file_upload_seq, db, logger)                                          

    while True:
        new_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv" 
        new_file_path = os.path.join(config.get("validity_period_extend_path"), new_file_name)
        if not os.path.exists(new_file_path):
            break
        time.sleep(1)

    os.makedirs(config.get("validity_period_extend_path"), exist_ok=True)
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


            # Generate an error file
        error_file_name = f"{file_type}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.err.csv"
        error_file_path = os.path.join(config.get("validity_period_extend_path"), error_file_name)

        with open(new_file_path, "r", encoding="utf-8-sig") as record_file, \
             open(error_file_path, "w", encoding="utf-8-sig", newline="") as error_file:

                file_reader = csv.reader(record_file)
                file_writer = csv.writer(error_file)

                for row_number, row in enumerate(file_reader, start=1):
                    if not row:  # Skip empty lines
                        continue

                    logger.info(f"Processing row {row_number}: {row}")

                    try:
                        case_id = int(row[0].strip()) if len(row) > 0 and row[0].strip().isdigit() else None
                        account_no = int(row[1].strip()) if len(row) > 1 and row[1].strip().isdigit() else None
                        telephone_no = row[2].strip() if len(row) > 2 else ""
                        no_of_months = int(row[3].strip()) if len(row) > 3 and row[3].strip().isdigit() else None

                        # Check if all case_id, account_no, and telephone_no are missing
                        if not case_id and not account_no and not telephone_no:
                            logger.warning(f"Skipping row {row_number} as all Case ID, Account No, and Telephone No are missing.")
                            # Write the row to error file
                            row.append("|All Case ID, Account No, and Telephone No missing")
                            file_writer.writerow(row)
                            raise MissingIdentifiersException(row_number)

                        # Handle missing no of months
                        if not no_of_months:
                            logger.warning(f"Skipping row {row_number} due to missing no of months.")
                            row.append("|No of months missing")
                            file_writer.writerow(row)
                            raise MissingNoOfMonthsException(row_number)

                        # Build query conditions dynamically
                        query = {}
                        if case_id:
                            query["case_id"] = case_id
                        if account_no:
                            query["account_no"] = account_no
                        if telephone_no:
                            query["ref_products.0.product_label"] = telephone_no

                        # Perform the database query
                        case_details_collection = db["Case_details"]
                        case_document = case_details_collection.find_one(query)

                        # Check if a matching case is found
                        if not case_document:
                            logger.warning(f"Skipping row {row_number} as no case found for provided identifiers.")
                            row.append("|No case found")
                            file_writer.writerow(row)
                            raise CaseNotFoundException(row_number)

                        # Get the current case status value from database
                        case_status_value = case_document.get("case_status", [{}])[0].get("case_status", None)

                        # Call the get-case-phase API to get the case phase
                        try:
                            case_phase_response = requests.get(config.get("get_case_phase_endpoint"), params={"case_status": case_status_value})

                            if not case_phase_response.status_code == 200:
                                logger.warning(f"Skipping row {row_number} as an error occurred in get-case-phase API")
                                row.append("|Error in get-case-phase API")
                                file_writer.writerow(row)
                                raise CasePhaseException(row_number)

                        except DataProcessingException as e:
                            logger.error(f"Error processing row {row_number}: {str(e)}")
                            continue

                        # Call the Case_Phase API to check the phase
                        case_phase_response = case_phase_response.json()

                        if "case_phase" not in case_phase_response:
                            logger.warning(f"Skipping row {row_number} as the API didn't return a valid case phase")
                            row.append("|Invalid case status")
                            file_writer.writerow(row)
                            raise InvalidCaseStatusException(row_number, case_status_value)

                        case_phase = case_phase_response["case_phase"]

                        # Check if the case phase in "Negotiation" or "Mediation Board" phase
                        if case_phase not in ["Negotiation", "Mediation Board"]:
                            logger.warning(f"Skipping row {row_number} as case_status {case_status_value} is not in the Negotiation or Mediation Board phases.")
                            row.append("|Invalid case status")
                            file_writer.writerow(row)
                            raise InvalidCaseStatusException(row_number, case_status_value)

                        # check if period extension exceeds 5 months
                        monitor_months = case_document.get("monitor_months", 0)
                        if (no_of_months + monitor_months) > config.get("max_monitor_months"):
                            logger.warning(f"Skipping row {row_number} as period extension exceeds 5 months.")
                            row.append("|Exceeds 5 months")
                            file_writer.writerow(row)
                            raise PeriodExtensionException(row_number)

                        # Update the monitor months
                        new_monitor_months = no_of_months + monitor_months
                        document_id = case_document["_id"]
                        case_details_collection.update_one({"_id": document_id}, {"$set": {"monitor_months": new_monitor_months}})

                        # Update the drc expire date
                        current_expire_dtm = case_document["dtm"][0]["expire_dtm"]
                        formatted_current_expire_dtm = datetime.fromisoformat(current_expire_dtm)
                        new_expire_dtm = formatted_current_expire_dtm + relativedelta(months=no_of_months)
                        case_details_collection.update_one({"_id": document_id}, {"$set": {"dtm.0.expire_dtm": new_expire_dtm.isoformat()}})

                        # Update the case status description
                        case_details_collection.update_one({"_id": document_id}, {"$set": {"case_status_description": "Validity period extended successfully"}})

                        logger.info(f"Successfully processed row: {row}")

                    except DataProcessingException as e:
                        logger.error(f"Error processing row {row_number}: {str(e)}")
                        continue

                    except FileDownloadError as e:
                        logger.error(f"Failed to download file: {file_path}")
                        continue

                    logger.info(f"Task {task_id}: Validity period extension process completed.")

            # Update task status
        file_upload_log_collection.update_one(
                {"file_upload_seq": file_upload_seq},
                {"$set": {
                    "log_status": "Completed",
                    "log_status_description": "Validity period extension completed",
                }},
            )
        logger.info(f"Task {task_id}: File upload log updated successfully.")


        system_tasks_collection.update_one(
                {"Task_Id": task_id},
                {"$set": {
                    "task_status": "Completed",
                    "task_status_description": "Validity period extension completed",
                }},
            )
        
        logger.info(f"Task {task_id}: System task updated successfully.")
        
        system_tasks_inprogress_collection.update_one(
                {"Task_Id": task_id},
                {"$set": {
                    "task_status": "Completed",
                    "task_status_description": "Validity period extension completed",
                }},
            )
        logger.info(f"Task {task_id}: System task in progress updated successfully.")

    except Exception as e:
            logger.error(f"Unexpected error occurred: {e}", exc_info=True)
            raise TaskProcessingError(task_id, f"Unexpected error occurred: {e}", logger)

