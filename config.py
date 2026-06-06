# BOILERPLATE
import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
# LOGIC
class Config:
    """Typed configuration container populated from environment variables."""
    s3_bucket: str
    s3_input_prefix: str
    s3_error_prefix: str
    s3_report_prefix: str
    db_secret_id: str
    sns_success_topic_arn: str
    sns_failure_topic_arn: str
    pipeline_service_identity: str


# LOGIC
def _require_env(name: str) -> str:
    """
    Reads a required environment variable.
    Raises EnvironmentError immediately if the variable is absent or empty.
    """
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{name}' is missing or empty. "
            "Pipeline cannot start without it."
        )
    return value


# LOGIC
def _optional_env(name: str, default: str) -> str:
    """
    Reads an optional environment variable.
    Returns the provided default if the variable is absent or empty.
    """
    value = os.environ.get(name)
    if not value:
        logger.info(
            "Optional environment variable '%s' not set; using default '%s'.",
            name,
            default,
        )
        return default
    return value


# LOGIC
def load_config() -> Config:
    """
    Reads all environment variables required by the pipeline.
    Raises EnvironmentError at startup if any required variable is missing.
    Returns a frozen Config dataclass with typed fields.
    """
    logger.info("Loading pipeline configuration from environment variables.")

    # LOGIC — required variables: absence raises EnvironmentError immediately
    s3_bucket = _require_env("S3_BUCKET")
    db_secret_id = _require_env("DB_SECRET_ID")
    sns_success_topic_arn = _require_env("SNS_SUCCESS_TOPIC_ARN")
    sns_failure_topic_arn = _require_env("SNS_FAILURE_TOPIC_ARN")
    pipeline_service_identity = _require_env("PIPELINE_SERVICE_IDENTITY")

    # LOGIC — optional variables: defaults documented in approved design
    s3_input_prefix = _optional_env("S3_INPUT_PREFIX", "incoming/")
    s3_error_prefix = _optional_env("S3_ERROR_PREFIX", "errors/")
    s3_report_prefix = _optional_env("S3_REPORT_PREFIX", "reports/")

    config = Config(
        s3_bucket=s3_bucket,
        s3_input_prefix=s3_input_prefix,
        s3_error_prefix=s3_error_prefix,
        s3_report_prefix=s3_report_prefix,
        db_secret_id=db_secret_id,
        sns_success_topic_arn=sns_success_topic_arn,
        sns_failure_topic_arn=sns_failure_topic_arn,
        pipeline_service_identity=pipeline_service_identity,
    )

    logger.info(
        "Configuration loaded successfully. "
        "s3_bucket=%s, s3_input_prefix=%s, s3_error_prefix=%s, "
        "s3_report_prefix=%s, pipeline_service_identity=%s",
        config.s3_bucket,
        config.s3_input_prefix,
        config.s3_error_prefix,
        config.s3_report_prefix,
        config.pipeline_service_identity,
    )

    return config