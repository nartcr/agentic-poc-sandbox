# BOILERPLATE
class FileReadError(Exception):
    """Raised when a position file cannot be retrieved or parsed from S3."""

    # LOGIC
    def __init__(self, key: str, reason: str):
        self.key = key
        self.reason = reason
        super().__init__(f"FileReadError for key '{key}': {reason}")


class ValidationError(Exception):
    """Raised for structural failures in validator.py (not per-row rejections)."""

    # LOGIC
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"ValidationError: {reason}")


class LoadError(Exception):
    """Raised when the database load operation fails."""

    # LOGIC
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"LoadError: {reason}")


class ErrorWriteError(Exception):
    """Raised when writing the error CSV to S3 fails."""

    # LOGIC
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"ErrorWriteError: {reason}")


class ReportWriteError(Exception):
    """Raised when writing the JSON report to S3 fails."""

    # LOGIC
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"ReportWriteError: {reason}")


class NotificationError(Exception):
    """Raised when an SNS publish call fails."""

    # LOGIC
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"NotificationError: {reason}")