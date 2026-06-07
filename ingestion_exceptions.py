# BOILERPLATE — custom exception hierarchy for the trade ingestion pipeline.
# No executable logic — class definitions only, as specified in the approved design.


class TradeIngestionError(Exception):
    """Base class for all trade ingestion pipeline errors.

    All pipeline-specific exceptions inherit from this class so that callers
    can either catch the entire hierarchy with a single ``except
    TradeIngestionError`` clause or target individual subtypes for finer-grained
    error handling.
    """


class FileReadError(TradeIngestionError):
    """Raised when the S3 object cannot be retrieved or read into a DataFrame.

    Covers conditions such as: object does not exist, bucket access denied,
    S3 service error, or the object body cannot be parsed as CSV.
    """


class FilenameParseError(TradeIngestionError):
    """Raised when the S3 object key does not match the expected filename pattern.

    Expected pattern: ``incoming/{desk_code}_{trade_date}_positions.csv``
    where ``trade_date`` is in ``YYYY-MM-DD`` format.  Any deviation causes
    this exception to be raised so the handler can write an audit record and
    publish a failure notification before exiting.
    """


class DBConnectionError(TradeIngestionError):
    """Raised when a connection to the Aurora PostgreSQL database cannot be established.

    Covers conditions such as: Secrets Manager unreachable, secret JSON
    malformed, psycopg2 connect() failure, or network timeout reaching the
    Aurora cluster endpoint.
    """


class ValidationError(TradeIngestionError):
    """Raised when the entire file fails a structural validation check.

    Distinct from per-row rejection (which is handled silently by moving the
    row to the rejected set).  This exception is reserved for file-level
    failures such as a mandatory column being absent from the CSV header
    entirely, making row-level processing impossible.

    The exception message should include the reason code in the form
    ``MISSING_COLUMN:{column_name}`` so the audit record and SNS failure
    notification carry a specific, actionable reason.
    """