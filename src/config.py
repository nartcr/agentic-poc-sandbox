import os  # BOILERPLATE

# BOILERPLATE — read all required environment variables at import time.
# Any missing variable raises KeyError immediately, giving a clear failure message.

DB_SECRET_ID: str = os.environ["DB_SECRET_ID"]
S3_BUCKET: str = os.environ["S3_BUCKET"]
SNS_SUCCESS_TOPIC_ARN: str = os.environ["SNS_SUCCESS_TOPIC_ARN"]
SNS_FAILURE_TOPIC_ARN: str = os.environ["SNS_FAILURE_TOPIC_ARN"]
AWS_REGION: str = os.environ["AWS_REGION"]

# LOGIC — static path prefixes matching the S3 data contract.
S3_INPUT_PREFIX: str = "incoming/"
S3_ERROR_PREFIX: str = "errors/"
S3_REPORT_PREFIX: str = "reports/"

# LOGIC — database identifiers matching the data contract table schemas.
DB_NAME: str = "app"
DB_SCHEMA: str = "demo_schema"

# BOILERPLATE — timezone constant used for all ET timestamps throughout the pipeline.
TIMEZONE: str = "America/Toronto"

# LOGIC — deduplication key matching the primary key of demo_schema.trade_positions.
DEDUP_COLUMNS: tuple = ("trade_id", "desk_code", "trade_date")

# LOGIC — mandatory fields required in every incoming CSV row (TAC-2 / BAC-2).
MANDATORY_FIELDS: list = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]