# BOILERPLATE — custom exception definitions for structured pipeline error handling

"""
Custom exception classes for the trade position processing pipeline.
All pipeline modules raise these typed exceptions so the Lambda handler
can distinguish error categories and route to the correct SNS topic or
audit status.
"""


# LOGIC — FileReadError: raised by file_reader when S3 retrieval or CSV parsing fails
class FileReadError(Exception):
    """
    Raised when the trade position CSV file cannot be retrieved from S3
    or cannot be parsed as a valid CSV.
    """


# LOGIC — CredentialError: raised by db_secrets when Secrets Manager retrieval fails
class CredentialError(Exception):
    """
    Raised when database credentials cannot be fetched from Secrets Manager
    or the secret is missing one or more required keys (host, port, dbname,
    username, password).
    """


# LOGIC — ValidationError: raised for unrecoverable validation failures (e.g., missing required columns in header)
class ValidationError(Exception):
    """
    Raised when the input file fails a structural validation check that
    prevents row-level processing from proceeding (e.g., required columns
    are absent from the CSV header entirely).
    """


# LOGIC — DatabaseError: raised by db_loader or audit_writer on unrecoverable database errors
class DatabaseError(Exception):
    """
    Raised when a database operation fails in a way that cannot be recovered
    from within the current pipeline invocation (e.g., connection failure,
    schema mismatch, or executor error on the trade_positions insert).
    """