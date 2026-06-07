import os
import logging
from dataclasses import dataclass

# BOILERPLATE
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # LOGIC — all fields sourced from environment variables, no hardcoded values
    s3_bucket: str
    s3_input_prefix: str
    s3_error_prefix: str
    s3_report_prefix: str
    db_secret_id: str
    db_name: str
    db_schema: str
    sns_success_topic_arn: str
    sns_failure_topic_arn: str


def load_config() -> Config:
    # LOGIC — reads all required environment variables at call time; missing vars raise KeyError (fail-fast)
    config = Config(
        s3_bucket=os.environ["S3_BUCKET"],
        s3_input_prefix=os.environ.get("S3_INPUT_PREFIX", "incoming/"),
        s3_error_prefix=os.environ.get("S3_ERROR_PREFIX", "errors/"),
        s3_report_prefix=os.environ.get("S3_REPORT_PREFIX", "reports/"),
        db_secret_id=os.environ["DB_SECRET_ID"],
        db_name=os.environ.get("DB_NAME", "app"),
        db_schema=os.environ.get("DB_SCHEMA", "demo_schema"),
        sns_success_topic_arn=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        sns_failure_topic_arn=os.environ["SNS_FAILURE_TOPIC_ARN"],
    )
    logger.info(
        "Config loaded: bucket=%s input_prefix=%s error_prefix=%s report_prefix=%s db_schema=%s",
        config.s3_bucket,
        config.s3_input_prefix,
        config.s3_error_prefix,
        config.s3_report_prefix,
        config.db_schema,
    )
    return config


# BOILERPLATE — module-level singleton loaded at import time
CONFIG: Config = load_config()