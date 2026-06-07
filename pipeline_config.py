# BOILERPLATE
import os

# LOGIC — read and validate all required environment variables at import time.
# Raises EnvironmentError immediately if any required variable is absent,
# preventing silent misconfiguration from propagating into pipeline logic.

def _require_env(name: str) -> str:
    """Return the value of environment variable `name` or raise EnvironmentError."""
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set or is empty."
        )
    return value


# LOGIC — required variables (hard failure if absent)
S3_BUCKET: str = _require_env("S3_BUCKET")
DB_SECRET_ID: str = _require_env("DB_SECRET_ID")
SNS_TOPIC_ARN_SUCCESS: str = _require_env("SNS_TOPIC_ARN_SUCCESS")
SNS_TOPIC_ARN_FAILURE: str = _require_env("SNS_TOPIC_ARN_FAILURE")

# LOGIC — optional variable with documented default (design: "demo_schema")
DB_SCHEMA: str = os.environ.get("DB_SCHEMA", "demo_schema")

# LOGIC — derived path constants (literal values fixed by data contract;
# centralised here so other modules never repeat raw string prefixes).
S3_INPUT_PREFIX: str = "incoming/"
S3_ERROR_PREFIX: str = "errors/"
S3_REPORT_PREFIX: str = "reports/"