# BOILERPLATE — custom exception declarations, no logic


class FileReadError(Exception):
    """Raised when an S3 file cannot be downloaded or parsed as CSV."""


class LoadError(Exception):
    """Raised when a database load operation fails."""


class ValidationError(Exception):
    """Raised when a validation operation encounters an unrecoverable error."""


class AuditWriteError(Exception):
    """Raised when writing an audit record to the database fails."""