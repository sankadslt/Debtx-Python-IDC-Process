


def update_status_to_inprogress(task_id, file_upload_seq, db, logger):
    system_tasks_collection = db["System_tasks"]
    system_tasks_inprogress_collection = db["System_tasks_Inprogress"]
    file_upload_log_collection = db["file_upload_log"]

    # Update task status in both collections
    system_tasks_collection.update_one(
        {"Task_Id": task_id},
        {"$set": {"task_status": "inProgress"}}
    )
    logger.info(f"Task {task_id} status set to 'inProgress' in system_tasks.")

    system_tasks_inprogress_collection.update_one(
        {"Task_Id": task_id},
        {"$set": {"task_status": "inProgress"}}
    )
    logger.info(f"Task {task_id} status set to 'inProgress' in system_tasks_inprogress.")

    # Update file upload log status
    file_upload_log_collection.update_one(
        {"file_upload_seq": file_upload_seq},
        {"$set": {"log_status": "inProgress"}}
    )
    logger.info(f"File upload log {file_upload_seq} status set to 'inProgress'.")

