# BOILERPLATE
import io
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC — column ordering for the error CSV output matches the data contract:
# all original input columns followed by the two appended columns
_ORIGINAL_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]
_APPENDED_COLUMNS = ["rejection_reason", "source_row_number"]


def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date: str,
    processed_at_et: datetime,
) -> "str | None":
    # LOGIC — skip write entirely when there are no rejected rows
    if rejected_df.empty:
        logger.info("No rejected rows — error file skipped")
        return None

    # LOGIC — build the S3 key using the ET timestamp formatted as YYYYMMDDTHHMMSS
    et_tz = pytz.timezone("America/Toronto")
    if processed_at_et.tzinfo is None:
        # LOGIC — if naive datetime supplied, localise to ET
        processed_at_et = et_tz.localize(processed_at_et)

    timestamp_str = processed_at_et.strftime("%Y%m%dT%H%M%S")
    s3_key = f"errors/{desk_code}_{trade_date}_positions_errors_{timestamp_str}.csv"

    # LOGIC — determine final column order: original columns present in the
    # rejected DataFrame + any extra columns + appended columns
    base_cols = [c for c in _ORIGINAL_COLUMNS if c in rejected_df.columns]
    extra_cols = [
        c for c in rejected_df.columns
        if c not in _ORIGINAL_COLUMNS and c not in _APPENDED_COLUMNS
    ]
    appended = [c for c in _APPENDED_COLUMNS if c in rejected_df.columns]
    ordered_columns = base_cols + extra_cols + appended

    output_df = rejected_df[ordered_columns].copy()

    # LOGIC — serialise to UTF-8 CSV in memory; no filesystem writes
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3 using existing bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows).",
        bucket,
        s3_key,
        len(output_df),
    )
    return s3_key