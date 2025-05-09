from actionManipulation.CaseDistributionPlannner.database.fetchDB import case_list_fetch_with_rtom, \
    get_current_arrears_amount, fetch_case_rtom, get_data_from_case_distribution
from actionManipulation.CaseDistributionPlannner.database.insertDB import insert_case_distribution

#get case list
if __name__ == "__main__":
    # Define test parameters
    drc_commision_rule = "PEO TV"
    arrears_band = "AB-5_10"
    task_created_dtm = "2025-5-18T00:00:00.000+00:00"

    # Fetch cases based on filters
    case_data = case_list_fetch_with_rtom(drc_commision_rule, arrears_band, task_created_dtm)

    # Print the retrieved case data
    print("Filtered cases:", case_data)

if __name__ == "__main__":
    print(get_current_arrears_amount(12))

if __name__ == "__main__":
    print(fetch_case_rtom(12))



if __name__ == "__main__":
    # Sample case distribution data: mapping case IDs to DRC ID and RTOM
    case_distribution_data = {
        4: ['D1', 'CW'],
        8: ['D2', 'AG'],
        3: ['D1', 'CW'],
        7: ['D3', 'CW'],
        2: ['D1', 'GP'],
        6: ['D2', 'AG'],
        1: ['D1', 'CW'],
        5: ['D2', 'AD']
    }

    case_distribution_batch_id = 4

    # Call the function to insert case distribution data
    insert_case_distribution(case_distribution_data, case_distribution_batch_id,2)


if __name__ == "__main__":
    print(get_data_from_case_distribution(4))

# Example execution when the script runs directly
if __name__ == '__main__':
    # Define input parameters
    task_created_dtm = "2025-5-18T00:00:00.000+00:00"
    arrears_band = "AB-5_10"
    drc_commision_rule = "PEO TV"
    case_distribution_batch_id = 11
    batch_seq = 1
    distributed_DRC = {"D1": 4, "D2": 8, "D3": 12}

    # Mapping of DRCs to RTOMs
    drc_rtom_mapping = {
        "D1": ["AD", "GP", "CW"],
        "D2": ["AD", "AG"],
        "D3": ["CW", "GP", "AD"]
    }

    # Call the function to assign cases to DRCs
    logger.info("Executing DRC case assignment script.")
    drc_assign_case(task_created_dtm, arrears_band, drc_commision_rule, case_distribution_batch_id, batch_seq,
                    distributed_DRC, drc_rtom_mapping)
    logger.info("DRC case assignment script execution completed.")

if __name__ == "__main__":
   cases = [(1, 'CW'), (2, 'GP'), (3, 'CW'), (4, 'CW'), (5, 'AD'), (6, 'AG'), (7, 'CW'), (8, 'AG'), (9, 'KD'), (10, 'XX'), (11, 'YY'), (12, 'WW')]
   distributed_DRC = {"D1": 4, "D2": 8, "D3": 12,"D4":10}
   drc_rtom_mapping = {
      "D1": ["AD", "GP", "CW"],
      "D2": ["AD", "AG"],
      "D3": ["CW", "GP", "AD"],
      "D4": ["XX","YY","WW"]
   }

   x=case_distribution(cases,distributed_DRC,drc_rtom_mapping)
   print(x)