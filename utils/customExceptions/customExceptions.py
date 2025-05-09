from utils.db import db  # Singleton instance
from utils.coreConfig import config
from utils.logger.loggers import get_logger

logger = get_logger("custom_exceptions")

# Collections (shared globally)
system_tasks_collection = db["System_tasks"]
file_upload_log_collection = db["file_upload_log"]


class TaskProcessingError(Exception):
    """Exception raised for errors in task processing."""
    def __init__(self, task_id, error_message, logger, log_status="Failed", error_count=1):
        self.task_id = task_id
        self.error_message = error_message
        self.log_status = log_status
        self.error_count = error_count
        super().__init__(self.error_message)

        # Log the error
        logger.error(self.error_message)

        try:
            # Update system_tasks
            system_tasks_collection.update_one(
                {"_id": self.task_id},
                {"$set": {
                    "task_status": "Error",
                    "task_status_description": self.error_message
                }}
            )

            # Update file_upload_log
            file_upload_log_collection.update_one(
                {"file_upload_seq": self.task_id},
                {"$set": {
                    "log_status": self.log_status,
                    "error_count": self.error_count,
                    "log_status_description": self.error_message
                }}
            )

        except Exception as db_error:
            logger.exception(f"Failed to log TaskProcessingError to DB: {db_error}")


class ValidationError(Exception):
    """Custom exception for validation errors in incident creation."""
    def __init__(self, record_number, field_name, message):
        self.record_number = record_number
        self.field_name = field_name
        self.message = message
        super().__init__(f"Record {record_number}: {field_name} - {message}")


class FileDownloadError(Exception):
    """Exception raised for errors in the file download process."""
    def __init__(self, file_path, status_code, message="Failed to download file"):
        self.file_path = file_path
        self.status_code = status_code
        self.message = f"{message}: {file_path} (Status Code: {status_code})"
        super().__init__(self.message)


class DataProcessingException(Exception):
    """Base exception for data processing errors in validity period extension."""
    def __init__(self, row_number, message):
        self.row_number = row_number
        self.message = f"Row {row_number}: {message}"
        super().__init__(self.message)


class MissingIdentifiersException(DataProcessingException):
    def __init__(self, row_number):
        super().__init__(row_number, "Missing values for all Case ID, Account No, and Telephone No.")


class CasePhaseException(DataProcessingException):
    def __init__(self, row_number):
        super().__init__(row_number, "Error occurred in get-case-phase API.")


class MissingNoOfMonthsException(DataProcessingException):
    def __init__(self, row_number):
        super().__init__(row_number, "Missing value for 'No of Months'.")


class CaseNotFoundException(DataProcessingException):
    def __init__(self, row_number):
        super().__init__(row_number, "No case found for provided identifiers.")


class InvalidCaseStatusException(DataProcessingException):
    def __init__(self, row_number, case_status):
        super().__init__(row_number, f"Invalid case status '{case_status}'. Expected: Negotiation or Mediation Board.")


class PeriodExtensionException(DataProcessingException):
    def __init__(self, row_number):
        super().__init__(row_number, "Period extension exceeds 5 months.")
