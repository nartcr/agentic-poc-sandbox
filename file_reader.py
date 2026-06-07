# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns that must be present in the CSV header
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_position_file(bucket: str, key: str) -> pd.DataFrame:
    """
    LOGIC: Download the CSV file from S3 and return a raw pandas DataFrame
    with all columns typed as str. Validates that the file is non-empty and
    contains at minimum the expected mandatory columns.

    Args:
        bucket: S3 bucket name (from caller; must equal os.environ["S3_BUCKET"])
        key: Full S3 object key, e.g. 'incoming/DESK_2024-01-15_positions.csv'

    Returns:
        pd.DataFrame with all columns as dtype str, preserving original column
        names exactly as they appear in the file header.

    Raises:
        ValueError: If the file has zero data rows after the header, or if any
                    mandatory column is absent from the header.
        RuntimeError: If the S3 GetObject call fails for any reason.
    """
    # BOILERPLATE — build S3 client; bucket name confirmed via env var for safety
    s3_bucket = os.environ["S3_BUCKET"]
    if bucket != s3_bucket:
        logger.warning(
            "Caller-provided bucket '%s' differs from env S3_BUCKET '%s'; "
            "using caller-provided bucket as the event is authoritative.",
            bucket,
            s3_bucket,
        )

    logger.info("Downloading s3://%s/%s", bucket, key)

    # LOGIC — retrieve object from S3
    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_bytes = response["Body"].read()
    except Exception as exc:
        logger.error("Failed to download s3://%s/%s: %s", bucket, key, exc)
        raise RuntimeError(f"Could not read S3 object s3://{bucket}/{key}: {exc}") from exc

    logger.info("Downloaded %d bytes from s3://%s/%s", len(raw_bytes), bucket, key)

    # LOGIC — parse CSV; force all columns to str to avoid pandas type inference
    try:
        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            dtype=str,          # LOGIC: preserve raw string values; no type coercion here
            keep_default_na=False,  # LOGIC: treat empty cells as empty strings, not NaN
            na_filter=False,    # LOGIC: disable NA detection so blanks stay as ""
        )
    except Exception as exc:
        logger.error("Failed to parse CSV from s3://%s/%s: %s", bucket, key, exc)
        raise ValueError(f"Could not parse CSV file s3://{bucket}/{key}: {exc}") from exc

    # LOGIC — validate that the file contains at least one data row
    if len(df) == 0:
        raise ValueError(
            f"File s3://{bucket}/{key} contains zero data rows after the header."
        )

    logger.info("Raw rows read: %d from s3://%s/%s", len(df), bucket, key)
    logger.info("Columns found in file: %s", list(df.columns))

    # LOGIC — validate that all mandatory columns are present in the header
    missing_columns = [col for col in _MANDATORY_COLUMNS if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"File s3://{bucket}/{key} is missing mandatory columns: {missing_columns}"
        )

    # LOGIC — strip leading/trailing whitespace from column names only (not cell values)
    # Cell-level whitespace is handled by row_validator so rejection reasons are accurate.
    df.columns = [col.strip() for col in df.columns]

    logger.info(
        "Successfully read %d rows with %d columns from s3://%s/%s",
        len(df),
        len(df.columns),
        bucket,
        key,
    )

    return df