import os
import logging
from dataclasses import dataclass

# BOILERPLATE
logger = logging.getLogger(__name__)


# LOGIC
@dataclass
class Config:
    db_secret_id: str
    s3_bucket: str
    s3_input_prefix: str
    s3_error_prefix: str
    s3_report_prefix: str
    sns_topic_arn: str
    aws_region: str


def _load_config() -> Config:
    # LOGIC — reads all required environment variables; raises KeyError immediately
    # if any are missing so the Lambda fails at cold start rather than mid-execution
    db_secret_id = os.environ["DB_SECRET_ID"]
    s3_bucket = os.environ["S3_BUCKET"]
    s3_input_prefix = os.environ["S3_INPUT_PREFIX"]
    s3_error_prefix = os.environ["S3_ERROR_PREFIX"]
    s3_report_prefix = os.environ["S3_REPORT_PREFIX"]
    sns_topic_arn = os.environ["SNS_TOPIC_ARN"]
    aws_region = os.environ["AWS_REGION"]

    logger.debug(
        "Config loaded: bucket=%s input_prefix=%s error_prefix=%s report_prefix=%s region=%s",
        s3_bucket,
        s3_input_prefix,
        s3_error_prefix,
        s3_report_prefix,
        aws_region,
    )

    return Config(
        db_secret_id=db_secret_id,
        s3_bucket=s3_bucket,
        s3_input_prefix=s3_input_prefix,
        s3_error_prefix=s3_error_prefix,
        s3_report_prefix=s3_report_prefix,
        sns_topic_arn=sns_topic_arn,
        aws_region=aws_region,
    )


# LOGIC — module-level singleton; evaluated once per Lambda container lifecycle
config = _load_config()