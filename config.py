# BOILERPLATE
import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Config:
    # LOGIC — typed fields matching every environment variable in the design
    s3_bucket: str
    s3_input_prefix: str
    s3_report_prefix: str
    s3_error_prefix: str
    db_secret_id: str
    sns_success_topic_arn: str
    sns_failure_topic_arn: str
    audit_table: str
    positions_table: str


def load_config() -> Config:
    # LOGIC — reads all required environment variables; raises KeyError on any missing var
    logger.debug("Loading configuration from environment variables.")

    config = Config(
        s3_bucket=os.environ["S3_BUCKET"],
        s3_input_prefix=os.environ["S3_INPUT_PREFIX"],
        s3_report_prefix=os.environ["S3_REPORT_PREFIX"],
        s3_error_prefix=os.environ["S3_ERROR_PREFIX"],
        db_secret_id=os.environ["DB_SECRET_ID"],
        sns_success_topic_arn=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        sns_failure_topic_arn=os.environ["SNS_FAILURE_TOPIC_ARN"],
        audit_table=os.environ["AUDIT_TABLE"],
        positions_table=os.environ["POSITIONS_TABLE"],
    )

    logger.info(
        "Configuration loaded: bucket=%s, input_prefix=%s, report_prefix=%s, "
        "error_prefix=%s, audit_table=%s, positions_table=%s",
        config.s3_bucket,
        config.s3_input_prefix,
        config.s3_report_prefix,
        config.s3_error_prefix,
        config.audit_table,
        config.positions_table,
    )

    return config