# LOGIC — custom exception hierarchy for the trade position ingestion pipeline


class FileReadError(Exception):
    """Raised when the S3 object cannot be retrieved or the CSV cannot be parsed."""


class ValidationError(Exception):
    """Raised when the entire DataFrame cannot be processed.

    Not used for row-level rejections — those are handled by returning a
    rejected_df from row_validator.validate_rows(). This exception signals
    a structural failure that prevents any validation from proceeding.
    """


class DatabaseLoadError(Exception):
    """Raised when psycopg2 fails to connect to or execute against the Aurora database."""


class SecretsRetrievalError(Exception):
    """Raised when the Secrets Manager call fails or the secret cannot be parsed."""


class FilenameParseError(Exception):
    """Raised when the S3 key does not match the expected filename convention.

    Expected pattern: incoming/{desk_code}_{trade_date}_positions.csv
    where trade_date is YYYY-MM-DD.
    """