# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — expected columns in the error CSV (original input columns + rejection_reason)
_ERROR_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def write_error_file(rejected_df: pd.DataFrame, desk_code: str, trade_date: str) -> str:
    # LOGIC — build the S3 key from the exact pattern in the data contract
    s3_bucket = os.environ["S3_BUCKET"]
    s3_key = f"errors/{desk_code}_{trade_date}_errors.csv"

    # LOGIC — ensure the DataFrame has the expected column layout before serialising;
    # if rejected_df is empty we still write a headers-only CSV for traceability.
    if rejected_df.empty:
        # LOGIC — construct an empty DataFrame with the correct headers
        output_df = pd.DataFrame(columns=_ERROR_COLUMNS)
    else:
        # LOGIC — only carry forward recognised columns; tolerate extra columns that
        # may have been attached upstream without raising.
        present_cols = [c for c in _ERROR_COLUMNS if c in rejected_df.columns]
        output_df = rejected_df[present_cols].copy()

    # LOGIC — serialise to UTF-8 CSV (no BOM, no index column)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3 using IAM role credentials (no hardcoded secrets)
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )

    logger.info(
        "Error file written: s3://%s/%s (%d rejected rows)",
        s3_bucket,
        s3_key,
        len(output_df),
    )

    # LOGIC — return the full S3 key so the caller can include it in the manifest
    return s3_key