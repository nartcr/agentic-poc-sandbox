# BOILERPLATE
import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    # BOILERPLATE — typed fields matching every env var in the design
    s3_bucket: str
    s3_input_prefix: str
    s3_error_prefix: str
    s3_report_prefix: str
    db_secret_id: str
    sns_success_arn: str
    sns_failure_arn: str
    db_schema: str
    db_name: str


def load_config() -> PipelineConfig:
    # LOGIC — read required variables first; raise immediately if absent
    required = {
        "S3_BUCKET": os.environ.get("S3_BUCKET"),
        "DB_SECRET_ID": os.environ.get("DB_SECRET_ID"),
        "SNS_SUCCESS_ARN": os.environ.get("SNS_SUCCESS_ARN"),
        "SNS_FAILURE_ARN": os.environ.get("SNS_FAILURE_ARN"),
    }

    # LOGIC — fail fast on any missing required variable
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(sorted(missing))}"
        )

    # LOGIC — optional variables use design-specified defaults
    config = PipelineConfig(
        s3_bucket=required["S3_BUCKET"],
        s3_input_prefix=os.environ.get("S3_INPUT_PREFIX", "incoming/"),
        s3_error_prefix=os.environ.get("S3_ERROR_PREFIX", "errors/"),
        s3_report_prefix=os.environ.get("S3_REPORT_PREFIX", "reports/"),
        db_secret_id=required["DB_SECRET_ID"],
        sns_success_arn=required["SNS_SUCCESS_ARN"],
        sns_failure_arn=required["SNS_FAILURE_ARN"],
        db_schema=os.environ.get("DB_SCHEMA", "demo_schema"),
        db_name=os.environ.get("DB_NAME", "app"),
    )

    logger.info(
        "PipelineConfig loaded: bucket=%s input_prefix=%s db_schema=%s",
        config.s3_bucket,
        config.s3_input_prefix,
        config.db_schema,
    )
    return config