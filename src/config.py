import os
import logging

# BOILERPLATE
logger = logging.getLogger(__name__)


# LOGIC — read required environment variables; raise EnvironmentError immediately if any are missing
def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set. "
            "Ensure all deployment configuration variables are present before starting."
        )
    return value


# LOGIC — module-level constants consumed by all other modules
S3_BUCKET: str = _require_env("S3_BUCKET")

S3_INPUT_PREFIX: str = os.environ.get("S3_INPUT_PREFIX", "incoming/")
S3_ERROR_PREFIX: str = os.environ.get("S3_ERROR_PREFIX", "errors/")
S3_REPORT_PREFIX: str = os.environ.get("S3_REPORT_PREFIX", "reports/")

DB_SECRET_ID: str = _require_env("DB_SECRET_ID")

SNS_SUCCESS_ARN: str = _require_env("SNS_SUCCESS_ARN")
SNS_FAILURE_ARN: str = _require_env("SNS_FAILURE_ARN")

AWS_REGION: str = _require_env("AWS_REGION")

logger.debug(
    "Configuration loaded: bucket=%s input_prefix=%s error_prefix=%s report_prefix=%s region=%s",
    S3_BUCKET,
    S3_INPUT_PREFIX,
    S3_ERROR_PREFIX,
    S3_REPORT_PREFIX,
    AWS_REGION,
)