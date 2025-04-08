class CustomException(Exception):
    """Base class for all custom exceptions."""
    def __init__(self, message="An error occurred"):
        super().__init__(message)
        
class DatabaseConnectionError(CustomException):
    """Raised when the connection to the database fails."""
    def __init__(self, message="Database connection failed"):
        super().__init__(message)
                

class DatabaseError(CustomException):
    """Raised when the connection to the database fails."""
    def __init__(self, message="Database connection failed"):
        super().__init__(message)

class ValidationError(CustomException):
    """Raised when a validation error occurs."""
    def __init__(self, message="Validation failed"):
        super().__init__(message)

class NotFoundError(CustomException):
    """Raised when a requested resource is not found."""
    def __init__(self, message="Requested data not found"):
        super().__init__(message)

class UnauthorizedError(CustomException):
    """Raised when authentication fails."""
    def __init__(self, message="Unauthorized access"):
        super().__init__(message)

class BadRequestError(CustomException):
    """Raised when an invalid request is made."""
    def __init__(self, message="Bad request"):
        super().__init__(message)

class DataFetchError(CustomException):
    """Raised when an error occurs while fetching the data."""
    def __init__(self, message="An error occurred while fetching the data"):
        super().__init__(message)

class DataInsertError(CustomException):
    """Raised when an error occurs while inserting data."""
    def __init__(self, message="An error occurred while inserting data"):
        super().__init__(message)

class NoValidDataError(CustomException):
    """Raised when no valid data is available for insertion."""
    def __init__(self, message="No valid data to insert"):
        super().__init__(message)

class ProcessingError(CustomException):
    """Raised when an error occurs while processing an individual task."""
    def __init__(self, message="Error processing task"):
        super().__init__(message)

class TaskIdNotFoundError(CustomException):
    """Raised when a Task ID is not found."""
    def __init__(self, message="Task ID not found"):
        super().__init__(message)

class CaseIdNotFoundError(CustomException):
    """Raised when a Case ID is not found."""
    def __init__(self, message="Case ID not found"):
        super().__init__(message)

class FileMissingError(CustomException):
    """Raised when a required file is missing."""
    def __init__(self, message="Required file not found"):
        super().__init__(message)

class DocumentUpdateError(CustomException):
    """Raised when a document update fails."""
    def __init__(self, message="Document update failed"):
        super().__init__(message)
class INIFileReadError():
    """Raised when there is an issue reading the INI file."""
    pass