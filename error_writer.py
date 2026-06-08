# BOILERPLATE
import io
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — exact column order for the error CSV as specified in the data contracts
_ERROR_CSV_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> Optional[str]:
    """
    # LOGIC
    Serialize rejected_df (including rejection_reason column) to CSV and upload
    to S3 at key errors/{desk_code}_{trade_date}_errors_{ts_str}.csv.
    Returns the full S3 key written, or None if rejected_df is empty.
    processing_ts must be ET-localized so strftime produces the correct ET timestamp.
    """

    # LOGIC — if no rejections, do nothing and return None
    if rejected_df is None or len(rejected_df) == 0:
        logger.info("No rejected rows — skipping error file write.")
        return None

    # LOGIC — format timestamp suffix from ET-localized datetime
    ts_str = processing_ts.strftime("%Y%m%dT%H%M%S")

    # LOGIC — construct S3 key using the exact pattern from the data contracts
    s3_key = f"errors/{desk_code}_{trade_date}_errors_{ts_str}.csv"

    # LOGIC — select and order columns per the error CSV contract;
    # only include columns that exist in the DataFrame to avoid KeyError
    output_cols = [c for c in _ERROR_CSV_COLUMNS if c in rejected_df.columns]
    # LOGIC — include any extra columns from the original input that are not in the
    # canonical list (preserves unexpected columns without silently dropping them)
    extra_cols = [c for c in rejected_df.columns if c not in output_cols]
    final_cols = output_cols + extra_cols
    output_df = rejected_df[final_cols]

    # LOGIC — serialize to CSV in memory (no temp files, no /tmp/ paths)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3 using existing bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written: s3://%s/%s (%d rejected rows)",
        bucket,
        s3_key,
        len(rejected_df),
    )

    return s3_key