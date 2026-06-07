# BOILERPLATE
import io
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC
def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    error_prefix: str,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
) -> str | None:
    """
    Write the rejected rows DataFrame to S3 as a UTF-8 CSV error file.
    Returns the S3 key written, or None if rejected_df is empty.

    S3 key pattern:
        {error_prefix}{desk_code}_{trade_date}_errors_{YYYYMMDDTHHmmss}.csv
    """
    # LOGIC — guard: nothing to write
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — build S3 key using ET-formatted timestamp
    et_tz = pytz.timezone("America/Toronto")
    if processing_timestamp.tzinfo is None:
        ts_et = et_tz.localize(processing_timestamp)
    else:
        ts_et = processing_timestamp.astimezone(et_tz)

    ts_str = ts_et.strftime("%Y%m%dT%H%M%S")
    s3_key = f"{error_prefix}{desk_code}_{trade_date}_errors_{ts_str}.csv"

    # LOGIC — define column order: all original input columns then rejection_reason last
    base_columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]
    # Include any extra columns that may exist in the DataFrame, preserving order,
    # but always put rejection_reason last.
    existing_base = [c for c in base_columns if c in rejected_df.columns]
    extra_columns = [
        c for c in rejected_df.columns
        if c not in base_columns and c != "rejection_reason"
    ]
    ordered_columns = existing_base + extra_columns + ["rejection_reason"]
    # Only select columns that actually exist in the DataFrame
    write_columns = [c for c in ordered_columns if c in rejected_df.columns]

    # LOGIC — serialise to CSV in memory (no /tmp/ paths)
    buffer = io.StringIO()
    rejected_df[write_columns].to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Wrote error file with %d rejected row(s) to s3://%s/%s",
        len(rejected_df),
        bucket,
        s3_key,
    )
    return s3_key