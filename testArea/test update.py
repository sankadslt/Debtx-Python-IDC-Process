from datetime import datetime

from utils.database.connectDB import get_db_connection


def update_Case_Distribution_DRC_Transactions(case_distribution_batch_id, processed_reject_case_count):
    db = get_db_connection()

    try:
        collection = db["Case_distribution_drc_transactions"]

        # Find the document by Case_Distribution_Batch_ID
        case_doc = collection.find_one({"Case_Distribution_Batch_ID": case_distribution_batch_id})

        if case_doc:
            batch_seq_details = case_doc.get("batch_seq_details", [])
            for batch in batch_seq_details:
                if batch.get("action_type") == "distribution":
                    batch["reject_case_count"] = processed_reject_case_count
                    batch["crd_distribution_status"] = "Processed"
                    batch["crd_distribution_status_on"] = datetime.now()

            # Update the summary status
            updated_data = {
                "$set": {
                    "batch_seq_details": batch_seq_details,
                    "summery_status": "Processed",
                    "status_changed_on": datetime.now()
                }
            }

            # Update the document
            result = collection.update_one({"Case_Distribution_Batch_ID": case_distribution_batch_id}, updated_data)

            if result.modified_count > 0:
                print("Document updated successfully")
            else:
                print("No changes were made to the document")
        else:
            print("Document not found")
    except Exception as e:
        print(f"Unexpected error: {e}")


# Example usage
update_Case_Distribution_DRC_Transactions(1, 5)