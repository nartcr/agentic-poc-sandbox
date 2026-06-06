# BOILERPLATE — custom exception hierarchy for the position ingestion pipeline


class FileReadError(Exception):
    """
    Raised when an S3 object cannot be retrieved or parsed by file_reader.py.
    Wraps underlying boto3 or pandas CSV parse errors.
    """
    # LOGIC — signals that the input file could not be read; triggers failure notification in main.py


class ValidationError(Exception):
    """
    Raised on unexpected validator failure in validator.py.
    Distinct from row-level rejections, which are handled by returning a rejected_df.
    Indicates a structural or programming error in the validation pipeline itself.
    """
    # LOGIC — signals an unrecoverable validator fault, not a data quality rejection


class LoadError(Exception):
    """
    Raised when the database INSERT transaction fails in loader.py.
    Wraps underlying psycopg2 errors after rollback has been attempted.
    """
    # LOGIC — signals that validated rows could not be persisted; triggers failure notification in main.py


class SecretsError(Exception):
    """
    Raised when AWS Secrets Manager cannot return the requested secret in secrets.py.
    Wraps boto3 client errors for secret retrieval failures.
    """
    # LOGIC — signals that runtime credentials are unavailable; any module depending on secrets will propagate this


class NotificationError(Exception):
    """
    Raised when an SNS publish call fails in notifier.py.
    Non-fatal by design: main.py logs and swallows this exception rather than re-raising.
    """
    # LOGIC — signals that the downstream notification could not be delivered; pipeline result is unaffected