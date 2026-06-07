# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — canonical column set as defined in the data contract
REQUIRED_COLUMNS = {
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
}

EXPECTED_COLUMN_ORDER = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_position_file(bucket: str, key: str) -> pd.DataFrame:
    # LOGIC — download CSV from S3 and return raw DataFrame with no type coercion
    logger.info("Fetching s3://%s/%s", bucket, key)

    # BOILERPLATE — S3 client; bucket name comes from caller (event-driven, not env var)
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise ValueError(
            f"Cannot retrieve s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — read body and enforce UTF-8 encoding
    raw_bytes: bytes = response["Body"].read()
    if not raw_bytes:
        raise ValueError(
            f"File s3://{bucket}/{key} is empty (zero bytes)"
        )

    try:
        raw_text: str = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"File s3://{bucket}/{key} is not valid UTF-8: {exc}"
        ) from exc

    # LOGIC — parse as comma-delimited CSV; dtype=str preserves raw string values
    try:
        df = pd.read_csv(
            io.StringIO(raw_text),
            sep=",",
            dtype=str,
            keep_default_na=False,  # prevent pandas from silently coercing empty strings to NaN
        )
    except pd.errors.EmptyDataError as exc:
        raise ValueError(
            f"File s3://{bucket}/{key} contains no parseable CSV data: {exc}"
        ) from exc
    except Exception as exc:
        raise ValueError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    logger.info(
        "Parsed CSV: %d rows, columns=%s",
        len(df),
        list(df.columns),
    )

    # LOGIC — validate that the file has at least one data row
    if len(df) == 0:
        raise ValueError(
            f"File s3://{bucket}/{key} has a header row but contains no data rows"
        )

    # LOGIC — check that all required column headers are present
    actual_columns = set(df.columns.str.strip())
    missing_columns = REQUIRED_COLUMNS - actual_columns
    if missing_columns:
        raise ValueError(
            f"File s3://{bucket}/{key} is missing required columns: "
            f"{sorted(missing_columns)}. "
            f"Found columns: {sorted(actual_columns)}"
        )

    # LOGIC — strip whitespace from column names to guard against trailing spaces in header
    df.columns = df.columns.str.strip()

    logger.info(
        "File validated: %d data rows, all required columns present",
        len(df),
    )
    return df