# BOILERPLATE — custom exception hierarchy; no logic, only class definitions

"""
Custom exception classes for the trade positions pipeline.
All pipeline modules raise these typed exceptions to enable
structured error propagation and targeted handling in the
Lambda entry point.
"""


class FileReadError(Exception):
    """Raised when the S3 file cannot be downloaded, parsed, or is empty."""


class FilenameParseError(ValueError):
    """
    Raised when the S3 object key does not match the expected pattern
    {desk_code}_{trade_date}_positions.csv.
    Inherits from ValueError because it represents a malformed input value.
    """


class ValidationError(Exception):
    """
    Raised when a structural or schema-level validation failure occurs
    that cannot be expressed as a per-row rejection (e.g., missing required
    columns entirely).
    """


class SecretFetchError(Exception):
    """
    Raised when the Secrets Manager call fails or the returned secret
    JSON is missing expected keys (host, port, dbname, username, password).
    """


class DatabaseLoadError(Exception):
    """
    Raised when the database insert transaction fails and cannot be
    recovered (e.g., connection failure, constraint violation outside
    the ON CONFLICT DO NOTHING path).
    """