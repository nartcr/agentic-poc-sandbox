# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — the seven mandatory columns defined in the data contract
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


class FileReadError(Exception):
    """
    # LOGIC
    Raised when the S3 CSV file is missing, empty, or cannot be parsed.
    """


def read_position_file(bucket: str, key: str) -> tuple:
    """
    # LOGIC
    Downloads the CSV file from S3 and reads it into a pandas DataFrame.
    Preserves all original column names (including any extra columns beyond
    the seven mandatory ones).

    Returns:
        (raw_df: pd.DataFrame, total_row_count: int)

    Raises:
        FileReadError: if the file is missing, empty, or cannot be parsed as CSV.
    """
    # BOILERPLATE — boto3 S3 client (no hardcoded credentials)
    s3_client = boto3.client("s3")

    # LOGIC — download the S3 object into memory
    logger.info("Downloading s3://%s/%s", bucket, key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        raise FileReadError(
            f"File not found in S3: s3://{bucket}/{key}"
        )
    except Exception as exc:
        raise FileReadError(
            f"Failed to retrieve s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — read the response body into an in-memory buffer (no /tmp/)
    try:
        body_bytes = response["Body"].read()
    except Exception as exc:
        raise FileReadError(
            f"Failed to read response body from s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — check for empty file before attempting CSV parse
    if not body_bytes or len(body_bytes) == 0:
        raise FileReadError(
            f"File is empty: s3://{bucket}/{key}"
        )

    # LOGIC — parse bytes as CSV into a pandas DataFrame
    try:
        raw_df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,          # preserve all columns as strings initially
            keep_default_na=False,  # do not silently convert empty strings to NaN
        )
    except Exception as exc:
        raise FileReadError(
            f"CSV parse failed for s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — reject an empty DataFrame (header-only file or zero data rows)
    if raw_df.empty:
        raise FileReadError(
            f"File contains no data rows (header only or empty): "
            f"s3://{bucket}/{key}"
        )

    # LOGIC — validate that all mandatory columns are present in the CSV header
    missing_cols = [c for c in MANDATORY_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        raise FileReadError(
            f"CSV is missing mandatory columns {missing_cols} in "
            f"s3://{bucket}/{key}"
        )

    total_row_count = len(raw_df)
    logger.info(
        "Successfully read %d rows from s3://%s/%s",
        total_row_count,
        bucket,
        key,
    )

    return raw_df, total_row_count