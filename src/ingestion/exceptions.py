# BOILERPLATE — custom exception hierarchy for the ingestion pipeline


class FileReadError(Exception):
    """Raised when an S3 object cannot be retrieved during file reading."""
    pass


class ValidationError(Exception):
    """Raised for structural validation failures (e.g., missing header columns).

    Not used for per-row rejections — those are handled by returning a
    rejected_df from validator.validate_rows.
    """
    pass


class LoadError(Exception):
    """Raised on unrecoverable database write failures during position loading."""
    pass


class ReportWriteError(Exception):
    """Raised if the S3 report write fails during report generation."""
    pass