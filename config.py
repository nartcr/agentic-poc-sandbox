# BOILERPLATE
import os

import pytz

# LOGIC — read and validate all required environment variables at import time


def _require_env(name: str) -> str:
    """Read a required environment variable or raise EnvironmentError."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set or is empty."
        )
    return value


S3_BUCKET = _require_env("S3_BUCKET")
S3_INPUT_PREFIX = _require_env("S3_INPUT_PREFIX")
S3_REPORTS_PREFIX = _require_env("S3_REPORTS_PREFIX")
S3_ERRORS_PREFIX = _require_env("S3_ERRORS_PREFIX")
DB_SECRET_ID = _require_env("DB_SECRET_ID")
SNS_TOPIC_ARN_SUCCESS = _require_env("SNS_TOPIC_ARN_SUCCESS")
SNS_TOPIC_ARN_FAILURE = _require_env("SNS_TOPIC_ARN_FAILURE")
AWS_REGION = _require_env("AWS_REGION")

# LOGIC — ET timezone constant used by all modules
TIMEZONE = pytz.timezone("America/Toronto")