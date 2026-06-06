# BOILERPLATE
import os
import logging
from dataclasses import dataclass

import pytz

logger = logging.getLogger(__name__)

# LOGIC — canonical Eastern Time timezone constant used throughout the pipeline
ET_TIMEZONE = pytz.timezone("America/Toronto")


@dataclass(frozen=True)
class Config:
    # LOGIC — typed fields for all environment variables; no caller reads os.environ directly
    s3_bucket: str
    s3_inbound_prefix: str
    s3_report_prefix: str
    s3_error_prefix: str
    db_secret_id: str
    app_secret_id: str
    sns_success_topic_arn: str
    sns_failure_topic_arn: str
    processing_service_id: str


def load_config() -> Config:
    # LOGIC — reads every required env var; raises KeyError immediately if any are absent
    logger.debug("Loading configuration from environment variables")
    cfg = Config(
        s3_bucket=os.environ["S3_BUCKET"],
        s3_inbound_prefix=os.environ["S3_INBOUND_PREFIX"],
        s3_report_prefix=os.environ["S3_REPORT_PREFIX"],
        s3_error_prefix=os.environ["S3_ERROR_PREFIX"],
        db_secret_id=os.environ["DB_SECRET_ID"],
        app_secret_id=os.environ["APP_SECRET_ID"],
        sns_success_topic_arn=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        sns_failure_topic_arn=os.environ["SNS_FAILURE_TOPIC_ARN"],
        processing_service_id=os.environ["PROCESSING_SERVICE_ID"],
    )
    logger.debug(
        "Configuration loaded: bucket=%s inbound_prefix=%s report_prefix=%s error_prefix=%s "
        "processing_service_id=%s",
        cfg.s3_bucket,
        cfg.s3_inbound_prefix,
        cfg.s3_report_prefix,
        cfg.s3_error_prefix,
        cfg.processing_service_id,
    )
    return cfg


# BOILERPLATE — module-level singleton; imported by all other modules as `from config import CONFIG`
CONFIG: Config = load_config()