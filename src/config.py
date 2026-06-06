import os
import pytz
from dataclasses import dataclass

# BOILERPLATE — timezone constant used across the pipeline
TIMEZONE = pytz.timezone("America/Toronto")

# LOGIC — S3 prefix constants matching the data contract
INPUT_PREFIX = "incoming/"
ERROR_PREFIX = "errors/"
REPORT_PREFIX = "reports/"

# LOGIC — fully-qualified table names as specified in the data contract
POSITIONS_TABLE = "demo_schema.trade_positions"
AUDIT_TABLE = "demo_schema.pipeline_audit"

# LOGIC — mandatory CSV columns that must be present and non-empty for every row
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


@dataclass
class Config:
    # LOGIC — all runtime configuration sourced from environment variables
    db_secret_id: str
    s3_bucket: str
    sns_success_arn: str
    sns_failure_arn: str
    db_schema: str
    db_name: str


def _load_config() -> Config:
    # LOGIC — reads every required env var; raises KeyError with the variable name if absent
    return Config(
        db_secret_id=os.environ["DB_SECRET_ID"],
        s3_bucket=os.environ["S3_BUCKET"],
        sns_success_arn=os.environ["SNS_SUCCESS_ARN"],
        sns_failure_arn=os.environ["SNS_FAILURE_ARN"],
        db_schema=os.environ.get("DB_SCHEMA", "demo_schema"),
        db_name=os.environ.get("DB_NAME", "app"),
    )


# BOILERPLATE — module-level singleton; populated once at import time
cfg: Config = _load_config()