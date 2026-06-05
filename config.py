# BOILERPLATE
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # LOGIC — typed container for all environment-sourced configuration
    S3_BUCKET: str
    S3_INPUT_PREFIX: str
    S3_REPORTS_PREFIX: str
    S3_ERRORS_PREFIX: str
    DB_SECRET_ID: str
    SNS_SUCCESS_TOPIC_ARN: str
    SNS_FAILURE_TOPIC_ARN: str
    AUDIT_TABLE: str
    TZ: str


def load_config() -> Config:
    # LOGIC — reads all required environment variables; raises KeyError on any missing required var
    return Config(
        S3_BUCKET=os.environ["S3_BUCKET"],
        S3_INPUT_PREFIX=os.environ["S3_INPUT_PREFIX"],
        S3_REPORTS_PREFIX=os.environ["S3_REPORTS_PREFIX"],
        S3_ERRORS_PREFIX=os.environ["S3_ERRORS_PREFIX"],
        DB_SECRET_ID=os.environ["DB_SECRET_ID"],
        SNS_SUCCESS_TOPIC_ARN=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        SNS_FAILURE_TOPIC_ARN=os.environ["SNS_FAILURE_TOPIC_ARN"],
        AUDIT_TABLE=os.environ.get("AUDIT_TABLE", "app.pipeline_audit"),
        TZ=os.environ["TZ"],
    )