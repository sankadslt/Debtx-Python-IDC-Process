from utils.database.connectDB import get_db_connection
from dateutil import parser  # Use dateutil to parse MongoDB date format

def case_list_fetch_with_rtom(drc_commision_rule, arrears_band, task_created_dtm):
    db = get_db_connection()
    if db is None:
        print("Database connection failed. Unable to fetch case distribution data.")
        return []

    try:
        collection = db["Case_details"]  # Replace with your collection name

        # Convert task_created_dtm string to a datetime object
        task_created_dt = parser.parse(task_created_dtm)  # Correctly parse ISO 8601 format

        # Query to filter cases based on drc_commision_rule, arrears_band, and created_dtm before task_created_dtm
        query = {
            "drc_commision_rule": drc_commision_rule,
            "arrears_band": arrears_band,
            "created_dtm": {"$lt": task_created_dt}  # Fetch cases created before task_created_dtm
        }

        # Fetch matching case_id, rtom, and created_dtm values
        cases = collection.find(query, {"case_id": 1, "rtom": 1, "created_dtm": 1, "_id": 0})

        case_list = []
        for case in cases:
            if case.get("rtom") is not None:  # Only keep cases where RTOM is present
                case_list.append((case["case_id"], case["rtom"]))
            else:
                print(f"Case {case['case_id']} removed due to missing RTOM")  # Print removed cases

        return case_list

    except Exception as e:
        print(f"Error fetching case IDs and RTOM: {e}")
        return []

# Example usage
if __name__ == "__main__":
    drc_commision_rule = "PEO TV"  # Filter cases with this commission rule
    arrears_band = "AB-5_10"  # Filter cases with this arrears band
    task_created_dtm = "2025-5-18T00:00:00.000+00:00"  # Get cases created before this timestamp

    case_data = case_list_fetch_with_rtom(drc_commision_rule, arrears_band, task_created_dtm)
    print("Filtered cases =", case_data)
