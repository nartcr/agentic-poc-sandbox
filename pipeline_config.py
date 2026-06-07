# BOILERPLATE
import os
import logging
from dataclasses import dataclass

import pytz  # noqa: F401 — imported here to enforce ET timezone dependency is present at config load

logger = logging.getLogger(__name__)

# LOGIC
REQUIRED_ENV_VARS = [
    "S3_BUCKET",
    "S3_INPUT_PREFIX",
    "S3_ERROR_PREFIX",
    "S3_REPORT_PREFIX",
    "DB_SECRET_ID",
    "SNS_SUCCESS_ARN",
    "SNS_FAILURE_ARN",
    "PIPELINE_SERVICE_NAME",
]


@dataclass(frozen=True)
class PipelineConfig:
    # LOGIC — typed fields matching every required environment variable
    s3_bucket: str
    s3_input_prefix: str
    s3_error_prefix: str
    s3_report_prefix: str
    db_secret_id: str
    sns_success_arn: str
    sns_failure_arn: str
    pipeline_service_name: str


def load_config() -> PipelineConfig:
    """
    Reads all required environment variables and returns a PipelineConfig.
    Collects ALL missing variables before raising so operators see every gap at once.
    Raises EnvironmentError if any required variable is absent.
    """
    # LOGIC — collect missing vars before raising
    missing = [var for var in REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    config = PipelineConfig(
        s3_bucket=os.environ["S3_BUCKET"],
        s3_input_prefix=os.environ["S3_INPUT_PREFIX"],
        s3_error_prefix=os.environ["S3_ERROR_PREFIX"],
        s3_report_prefix=os.environ["S3_REPORT_PREFIX"],
        db_secret_id=os.environ["DB_SECRET_ID"],
        sns_success_arn=os.environ["SNS_SUCCESS_ARN"],
        sns_failure_arn=os.environ["SNS_FAILURE_ARN"],
        pipeline_service_name=os.environ["PIPELINE_SERVICE_NAME"],
    )

    logger.info(
        "Pipeline configuration loaded. service=%s bucket=%s input_prefix=%s",
        config.pipeline_service_name,
        config.s3_bucket,
        config.s3_input_prefix,
    )
    return config