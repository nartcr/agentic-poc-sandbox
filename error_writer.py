# BOILERPLATE
import logging
import os
import re
from datetime import datetime

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns in the prescribed output order
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]

# LOGIC — regex for parsing the incoming source key
_SOURCE_KEY_RE = re.compile(
    r"^incoming/(?P<desk_code>[^_]+(?:_[^_]+)*)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _get_s3_client():  # BOILERPLATE
    return boto3.client("s3")


def _et_now() -> datetime:  # BOILERPLATE
    """Return current datetime in America/Toronto timezone."""
    return datetime.now(pytz.timezone("America/Toronto"))


def _parse_source_key(source_key: str) -> tuple[str, str]:
    """Extract desk_code and trade_date from the incoming S3 key using regex."""  # LOGIC
    match = _SOURCE_KEY_RE.match(source_key)
    if not match:
        raise ValueError(
            f"source_key does not match expected pattern "
            f"'incoming/{{desk_code}}_{{trade_date}}_positions.csv': {source_key!r}"
        )
    return match.group("desk_code"), match.group("trade_date")


def _enforce_column_order(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns so that the mandatory columns appear first (in prescribed order),
    followed by any additional columns from the source file.
    Columns listed in MANDATORY_COLUMNS that are absent from df are skipped silently.
    """  # LOGIC
    present_mandatory = [c for c in MANDATORY_COLUMNS if c in df.columns]
    extra_columns = [c for c in df.columns if c not in MANDATORY_COLUMNS]
    ordered_columns = present_mandatory + extra_columns
    return df[ordered_columns]


def write_error_file(bucket: str, source_key: str, rejected_df: pd.DataFrame) -> str:
    """
    Write rejected rows to a CSV file in S3 under errors/.
    Returns the S3 key of the written file, or an empty string if there are no rejected rows.
    """  # LOGIC

    # LOGIC — skip S3 write entirely when there are no rejected rows
    if rejected_df.empty:
        logger.info("No rejected rows; skipping error file write.")
        return ""

    desk_code, trade_date = _parse_source_key(source_key)

    now_et = _et_now()
    timestamp_str = now_et.strftime("%Y%m%d%H%M%S")

    # LOGIC — construct S3 key following the data contract pattern
    error_key = f"errors/{desk_code}_{trade_date}_{timestamp_str}_errors.csv"

    # LOGIC — enforce column order: mandatory columns first, extras after
    ordered_df = _enforce_column_order(rejected_df)

    # LOGIC — serialise to CSV bytes (UTF-8, with header row)
    csv_bytes = ordered_df.to_csv(index=False).encode("utf-8")

    # LOGIC — write to S3
    s3 = _get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows)",
        bucket,
        error_key,
        len(rejected_df),
    )
    return error_key