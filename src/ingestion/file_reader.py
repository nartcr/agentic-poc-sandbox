# BOILERPLATE
import io
import logging
import os
import re

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC
_FILENAME_PATTERN = re.compile(
    r"incoming/([A-Z0-9_]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)

_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _parse_filename(key: str) -> tuple:
    """
    # LOGIC
    Extracts (desk_code, trade_date) from the S3 object key.
    Raises ValueError if the key does not match the expected naming convention.
    """
    match = _FILENAME_PATTERN.search(key)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {key}")
    desk_code = match.group(1)
    trade_date = match.group(2)
    logger.info("Parsed filename — desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def read_position_file(s3_client, bucket: str, key: str) -> tuple:
    """
    # LOGIC
    Downloads the CSV at s3://bucket/key, validates the filename, parses it
    into a DataFrame, and returns (raw_df, desk_code, trade_date).

    Raises:
        ValueError: if the filename does not match the expected pattern.
        ValueError: if the file contains zero data rows.
    """
    # LOGIC — validate filename before touching S3 to fail fast
    desk_code, trade_date = _parse_filename(key)

    logger.info("Downloading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    # LOGIC — parse CSV in memory; dtype=str preserves raw values for validation
    raw_df = pd.read_csv(
        io.StringIO(body_bytes.decode("utf-8")),
        dtype=str,
        keep_default_na=False,
    )

    logger.info(
        "Read %d rows from s3://%s/%s", len(raw_df), bucket, key
    )

    # LOGIC — reject empty files immediately
    if len(raw_df) == 0:
        raise ValueError(f"File contains zero data rows: {key}")

    # LOGIC — warn about any missing mandatory columns (validator will catch per-row gaps)
    missing_cols = [c for c in _MANDATORY_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        logger.warning(
            "Input file is missing mandatory columns: %s", missing_cols
        )

    return raw_df, desk_code, trade_date