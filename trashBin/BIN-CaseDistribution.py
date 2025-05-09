from datetime import datetime
from pymongo import MongoClient
from utils.database.DB_Config import MONGO_URI, DB_NAME, CASE_DETAIL_COLLECTION_NAME

"""
This file is responsible for fetching data from MongoDB for processing.
Task Data fetched includes:
    - Role-based information
    - Area slabs
    - Distributed amounts (as an array)

Case Data fetched includes:
"""

# Mapping for Area Slabs and Arrears Amount Ranges
AREA_SLAB_RANGES = {
    "AB-5_10": (5000, 10000),
    "AB-10_25": (10000, 25000),
    "AB-25_50": (25000, 50000),
    "AB-50_100": (50000, 100000),
    "AB-100_999": (100000, 999000),
}


def Case_list_fetch(task_created_dtm, area_slab, drc_selection_rule):
    """
    Fetch case data from the case_details collection based on conditions.
    """
    try:
        # Validate Area Slab
        if area_slab not in AREA_SLAB_RANGES:
            raise ValueError(f"Invalid Area Slab: {area_slab}")

        # Get arrears amount range
        min_arrears, max_arrears = map(int, AREA_SLAB_RANGES[area_slab])

        # Check and actionManipulation task_created_dtm
        if isinstance(task_created_dtm, str):
            # If it's a string, normalize and convert it
            if task_created_dtm.endswith("Z"):
                task_created_dtm = task_created_dtm.replace("Z", "+00:00")
            task_created_dtm_obj = datetime.fromisoformat(task_created_dtm)
        elif isinstance(task_created_dtm, datetime):
            # If it's already a datetime object, use it directly
            task_created_dtm_obj = task_created_dtm
        else:
            raise ValueError(f"Unsupported type for task_created_dtm: {type(task_created_dtm)}")

        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        case_details_collection = db[CASE_DETAIL_COLLECTION_NAME]

        # Query to fetch cases
        query = {
            "created_dtm": {"$lte": task_created_dtm_obj},
            "case_current_status": "Open No Agent",
            "current_arrears_amount": {"$gte": min_arrears, "$lte": max_arrears},
            "drc_selection_rule": drc_selection_rule,
        }

        # Fetch and sort cases
        cases = case_details_collection.find(query).sort("current_arrears_amount", -1)

        # Process and return case IDs
        case_id_list = [case.get("case_id") for case in cases]
        client.close()
        return case_id_list

    except Exception as e:
        print(f"An error occurred in Case_list_fetch: {e}")
        return []

if __name__ == "__main__":
    # Example input values
    task_created_dtm = "2025-01-10 10:40:52.873000"
    area_slab = "AB-5_10"
    drc_selection_rule = "PEO TV"

    case_id_list = Case_list_fetch(task_created_dtm, area_slab, drc_selection_rule)
    if case_id_list:
        print("Case IDs:", case_id_list)
    else:
        print("No cases found or an error occurred.")
