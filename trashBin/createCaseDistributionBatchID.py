import time
import re
from utils.database.DB_Config import TMP_CASE_DISTRIBUTION_DRC
from utils.database.connectDB import get_db_connection

def generate_batch_id(latest_id):
    """
    Generate a new Case_Distribution_Batch_ID based on the latest ID.
    """
    match = re.search(r'BATCH(\d+)', latest_id)
    if match:
        current_id = int(match.group(1))  # Extract numeric part
        next_id = current_id + 1  # Increment the ID
    else:
        raise ValueError("Invalid latest_id format. Expected format: BATCH<number>_epoch.")

    epoch_number = int(time.time())  # Current epoch time
    batch_id = f"BATCH{next_id}_{epoch_number}"
    return batch_id

def get_next_batch_id():
    """
    Fetch the last Batch_ID and generate the next Batch_ID without updating the databaseOperations.
    """
    # Connect to the databaseOperations
    db = get_db_connection()
    if db is None:  # Explicitly check if db is None
        print("Database connection failed. Unable to retrieve batch ID.")
        return None

    collection = db[TMP_CASE_DISTRIBUTION_DRC]

    try:
        # Fetch the last document
        last_doc = collection.find_one(sort=[("_id", -1)])  # Sort by insertion order or a specific field
        if last_doc and "Case_Distribution_Batch_ID" in last_doc:
            latest_id = last_doc["Case_Distribution_Batch_ID"]
        else:
            latest_id = "BATCH0_0"  # Default for first entry if the collection is empty

        # Generate the next batch ID
        new_batch_id = generate_batch_id(latest_id)
        return new_batch_id

    except Exception as e:
        print(f"An error occurred while retrieving the batch ID: {e}")
        return None


# Main execution
if __name__ == "__main__":
    next_batch_id = get_next_batch_id()
    print("Next Batch ID:", next_batch_id)
