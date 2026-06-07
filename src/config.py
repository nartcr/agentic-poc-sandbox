# BOILERPLATE
import os
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# LOGIC
@dataclass(frozen=True)
class Config:
    """Typed, immutable configuration derived entirely from environment variables."""
    S3_BUCKET: str
    S3_INPUT_PREFIX: str
    S3_ERROR_PREFIX: str
    S3_REPORT_PREFIX: str
    DB_SECRET_ID: str
    SNS_SUCCESS_ARN: str
    SNS_FAILURE_ARN: str
    AWS_REGION: str


# LOGIC — read and validate all required env vars at module import time
def _load_config() -> Config:
    """
    Read every required environment variable.
    Raises EnvironmentError listing ALL missing variables so operators can fix
    everything in one deployment cycle.
    """
    required_vars = [
        "S3_BUCKET",
        "S3_INPUT_PREFIX",
        "S3_ERROR_PREFIX",
        "S3_REPORT_PREFIX",
        "DB_SECRET_ID",
        "SNS_SUCCESS_ARN",
        "SNS_FAILURE_ARN",
        "AWS_REGION",
    ]

    missing = [var for var in required_vars if not os.environ.get(var)]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(missing)}"
        )

    cfg = Config(
        S3_BUCKET=os.environ["S3_BUCKET"],
        S3_INPUT_PREFIX=os.environ["S3_INPUT_PREFIX"],
        S3_ERROR_PREFIX=os.environ["S3_ERROR_PREFIX"],
        S3_REPORT_PREFIX=os.environ["S3_REPORT_PREFIX"],
        DB_SECRET_ID=os.environ["DB_SECRET_ID"],
        SNS_SUCCESS_ARN=os.environ["SNS_SUCCESS_ARN"],
        SNS_FAILURE_ARN=os.environ["SNS_FAILURE_ARN"],
        AWS_REGION=os.environ["AWS_REGION"],
    )

    logger.info(
        "Configuration loaded: bucket=%s input_prefix=%s error_prefix=%s "
        "report_prefix=%s region=%s",
        cfg.S3_BUCKET,
        cfg.S3_INPUT_PREFIX,
        cfg.S3_ERROR_PREFIX,
        cfg.S3_REPORT_PREFIX,
        cfg.AWS_REGION,
    )

    return cfg


# BOILERPLATE — module-level singleton; imported by all other modules
config: Config = _load_config()