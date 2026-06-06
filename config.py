# BOILERPLATE
import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# LOGIC
_REQUIRED_ENV_VARS = [
    "S3_BUCKET",
    "S3_INPUT_PREFIX",
    "S3_REPORTS_PREFIX",
    "S3_ERRORS_PREFIX",
    "DB_SECRET_ID",
    "SNS_SUCCESS_TOPIC_ARN",
    "SNS_FAILURE_TOPIC_ARN",
    "AWS_REGION",
]


@dataclass(frozen=True)
class _Config:
    # LOGIC — all infrastructure handles read from environment
    S3_BUCKET: str
    S3_INPUT_PREFIX: str
    S3_REPORTS_PREFIX: str
    S3_ERRORS_PREFIX: str
    DB_SECRET_ID: str
    SNS_SUCCESS_TOPIC_ARN: str
    SNS_FAILURE_TOPIC_ARN: str
    AWS_REGION: str


def _load_config() -> _Config:
    # LOGIC — collect all missing vars before raising so operators see everything at once
    missing = [var for var in _REQUIRED_ENV_VARS if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    cfg = _Config(
        S3_BUCKET=os.environ["S3_BUCKET"],
        S3_INPUT_PREFIX=os.environ["S3_INPUT_PREFIX"],
        S3_REPORTS_PREFIX=os.environ["S3_REPORTS_PREFIX"],
        S3_ERRORS_PREFIX=os.environ["S3_ERRORS_PREFIX"],
        DB_SECRET_ID=os.environ["DB_SECRET_ID"],
        SNS_SUCCESS_TOPIC_ARN=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        SNS_FAILURE_TOPIC_ARN=os.environ["SNS_FAILURE_TOPIC_ARN"],
        AWS_REGION=os.environ["AWS_REGION"],
    )
    logger.info(
        "Configuration loaded: bucket=%s input_prefix=%s reports_prefix=%s "
        "errors_prefix=%s region=%s",
        cfg.S3_BUCKET,
        cfg.S3_INPUT_PREFIX,
        cfg.S3_REPORTS_PREFIX,
        cfg.S3_ERRORS_PREFIX,
        cfg.AWS_REGION,
    )
    return cfg


# LOGIC — module-level singleton; raises EnvironmentError on import if env is incomplete
Config: _Config = _load_config()