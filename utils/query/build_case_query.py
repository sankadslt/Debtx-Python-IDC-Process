#region-Template
''' 
    Purpose: This function used for querying the case details.
    Created Date: 2025-03-12
    Created By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)
    Last Modified Date: 2025-03-12
    Modified By: Iman Chandrasiri(iman.chandrasiri0810@gmail.com)      
    Version: Python 3.12
    Dependencies: 
    Related Files: case_hold.py.
    Notes: The `build_case_query` function dynamically constructs a MongoDB query to search for a case based on the provided identifiers:  

1. case_id- If provided, it adds a condition to match the exact `case_id` (converted to an integer).  
2. account_number- If provided, it adds a condition to match `account_no` (also converted to an integer).  
3. telephone_number-If provided, it adds a condition to check if the `telephone_number` exists in the `ref_products.product_label` field (as a string).  

If at least one identifier is provided, the function returns a query with an **`$or`** condition, 
it will match any case where **at least one** of the given identifiers exists. If no identifiers are given, it returns `None`, meaning no query is performed. 
    
'''    
#endregion

def build_case_query(case_id=None, account_number=None, telephone_number=None):
    """Dynamically constructs a MongoDB query for case lookup."""
    conditions = []

    if case_id and case_id.isdigit():
        conditions.append({"case_id": int(case_id)})
    if account_number and account_number.isdigit() and len(account_number)==10:
        conditions.append({"account_no": int(account_number)})
    if telephone_number and telephone_number.isdigit() and 9 <= len (telephone_number)<=12:
        conditions.append({"ref_products.product_label": str(telephone_number)})

    return {"$and": conditions} if conditions else None
