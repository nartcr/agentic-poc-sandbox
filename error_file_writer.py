# BOILERPLATE
import io
import logging
import os
import re
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex matches exactly: incoming/{desk_code}_{trade_date}_positions.csv
# desk_code may contain letters, digits, hyphens; trade_date is YYYY-MM-DD
_SOURCE_KEY_RE = re.compile(
    r"^incoming/(?P<desk_code>[A-Za-z0-9\-]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)

_ET_TZ = pytz.timezone("America/Toronto")


def _parse_source_key(source_key: str) -> tuple:
    # LOGIC — extract desk_code and trade_date from source S3 key using regex
    match = _SOURCE_KEY_RE.match(source_key)
    if not match:
        raise ValueError(
            f"source_key does not match expected pattern "
            f"'incoming/{{desk_code}}_{{trade_date}}_positions.csv': {source_key!r}"
        )
    return match.group("desk_code"), match.group("trade_date")


def _build_error_key(desk_code: str, trade_date: str, timestamp_et: datetime) -> str:
    # LOGIC — derive error file S3 key per data contract:
    # errors/{desk_code}_{trade_date}_positions_errors_{YYYYMMDDTHHMMSS}.csv
    ts_str = timestamp_et.strftime("%Y%m%dT%H%M%S")
    return f"errors/{desk_code}_{trade_date}_positions_errors_{ts_str}.csv"


def write_error_file(rejected_df: pd.DataFrame, source_key: str) -> str:
    """
    Serialise the rejected-rows DataFrame to CSV and upload to S3.

    Parameters
    ----------
    rejected_df : pd.DataFrame
        DataFrame containing rejected rows with a ``rejection_reason`` column.
    source_key : str
        The original S3 object key, e.g. ``incoming/DESK01_2026-06-15_positions.csv``.

    Returns
    -------
    str
        The full S3 key of the written error file.
    """
    # BOILERPLATE — resolve runtime config from environment
    bucket = os.environ["S3_BUCKET"]

    # LOGIC — parse desk_code and trade_date from source key
    desk_code, trade_date = _parse_source_key(source_key)

    # LOGIC — timestamp in Eastern Time for file naming
    now_et = datetime.now(_ET_TZ)
    error_key = _build_error_key(desk_code, trade_date, now_et)

    # LOGIC — serialise DataFrame to UTF-8 CSV bytes (with header row)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3 via existing bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
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