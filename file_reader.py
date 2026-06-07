# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# BOILERPLATE
# Expected CSV columns per the data contract for incoming position files.
# Validation of these columns is NOT performed here — this module reads only.
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_position_file(bucket: str, key: str) -> pd.DataFrame:
    # LOGIC
    # Download the S3 object identified by bucket + key.
    # Uses a fresh boto3 S3 client on each call; no client-level state is cached here.
    s3_client = boto3.client("s3")

    logger.info("Reading S3 object: s3://%s/%s", bucket, key)

    response = s3_client.get_object(Bucket=bucket, Key=key)

    # LOGIC
    # Read the raw bytes from the streaming body and wrap in an in-memory buffer.
    # This avoids any filesystem interaction and keeps data entirely in-memory.
    body_bytes = response["Body"].read()
    buffer = io.BytesIO(body_bytes)

    # LOGIC
    # Parse the CSV with pandas, forcing all columns to string (dtype=object).
    # This preserves whitespace, leading zeros, and any other raw content exactly
    # as it appears in the file so downstream validation can inspect the true values.
    # keep_default_na=False and na_values=[] prevent pandas from silently converting
    # empty strings or sentinel values into NaN before validation runs.
    df = pd.read_csv(
        buffer,
        dtype=str,
        keep_default_na=False,
        na_values=[],
        encoding="utf-8",
    )

    # LOGIC
    # Log the row count and column list at INFO level for operational visibility.
    logger.info(
        "File read complete: s3://%s/%s — %d rows, columns=%s",
        bucket,
        key,
        len(df),
        list(df.columns),
    )

    return df