# BOILERPLATE
import io
import logging
import os
import datetime

import boto3
import pandas as pd

from time_utils import format_et_compact

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — module-level S3 client
_s3_client = None


def _get_s3_client():
    # BOILERPLATE
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def write_errors(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: datetime.date,
    processing_timestamp: datetime.datetime,
):
    """
    Write rejected rows to S3 as a UTF-8 CSV with header row.
    Returns the S3 key of the written file, or None if rejected_df is empty.
    """
    # LOGIC — write nothing and return None when there are no rejected rows
    if rejected_df is None or rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s — skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    # BOILERPLATE
    bucket = os.environ["S3_BUCKET"]
    ts_compact = format_et_compact(processing_timestamp)

    # LOGIC — S3 key: errors/{desk_code}_{trade_date}_errors_{yyyymmddTHHMMSS}.csv
    error_key = f"errors/{desk_code}_{trade_date}_errors_{ts_compact}.csv"

    # LOGIC — serialise DataFrame to CSV in memory (no /tmp/ paths)
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    _get_s3_client().put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows, %d bytes)",
        bucket,
        error_key,
        len(rejected_df),
        len(csv_bytes),
    )

    return error_key