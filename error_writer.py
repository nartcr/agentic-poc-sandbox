# BOILERPLATE
import io
import json
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — canonical column order as specified in the data contract
_ERROR_FILE_COLUMNS = [
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
    trade_date_str: str,
) -> str:
    # LOGIC — construct the S3 key from the data contract pattern:
    #   errors/{desk_code}_{trade_date}_errors.csv
    s3_key = f"errors/{desk_code}_{trade_date_str}_errors.csv"

    logger.info(
        "Writing error file | bucket=%s | key=%s | rejected_rows=%d",
        bucket,
        s3_key,
        len(rejected_df),
    )

    # LOGIC — enforce canonical column order; add any missing columns as empty
    #   strings so the CSV always has a consistent header regardless of how
    #   malformed the input was.
    output_df = _coerce_to_error_schema(rejected_df)

    # LOGIC — serialize to CSV in memory; encode to UTF-8 bytes for S3 upload
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False, columns=_ERROR_FILE_COLUMNS)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3 using the Lambda execution role credentials
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file uploaded successfully | s3://%s/%s | bytes=%d",
        bucket,
        s3_key,
        len(csv_bytes),
    )

    return s3_key


def _coerce_to_error_schema(df: pd.DataFrame) -> pd.DataFrame:
    # LOGIC — reindex to the canonical column list; columns present in df are
    #   retained with their original values; missing columns are filled with "".
    #   This handles edge cases where a severely malformed input file causes
    #   some columns to be absent from the DataFrame entirely.
    if df.empty:
        # LOGIC — return a zero-row DataFrame with the correct columns so
        #   the CSV always contains a header row even when there are no
        #   rejected records.
        return pd.DataFrame(columns=_ERROR_FILE_COLUMNS)

    # LOGIC — work on a copy; avoid mutating the caller's DataFrame
    result = df.copy()

    for col in _ERROR_FILE_COLUMNS:
        if col not in result.columns:
            result[col] = ""

    # LOGIC — select and reorder to exactly the required columns
    result = result[_ERROR_FILE_COLUMNS]

    return result