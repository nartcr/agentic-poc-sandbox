# BOILERPLATE
import datetime
import io
import logging
import os
import re

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact regex from design spec: {desk_code}_{trade_date}_positions.csv
# desk_code: one or more alphanumeric characters
# trade_date: YYYY-MM-DD
_FILENAME_PATTERN = re.compile(
    r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# LOGIC — expected CSV columns per data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def parse_s3_file(bucket: str, key: str) -> tuple:
    # LOGIC — downloads CSV from S3, validates filename, returns (DataFrame, desk_code, trade_date)
    filename = os.path.basename(key)
    logger.info("Parsing S3 file: bucket=%s key=%s filename=%s", bucket, key, filename)

    desk_code, trade_date = _validate_filename(filename)
    logger.info("Filename validated: desk_code=%s trade_date=%s", desk_code, trade_date)

    s3_client = boto3.client("s3")
    logger.info("Downloading s3://%s/%s", bucket, key)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()
    logger.info("Downloaded %d bytes from S3", len(body_bytes))

    # LOGIC — read CSV from in-memory buffer; no filesystem writes
    buffer = io.BytesIO(body_bytes)
    raw_df = pd.read_csv(buffer, dtype=str)
    logger.info("Parsed CSV: %d rows, columns=%s", len(raw_df), list(raw_df.columns))

    # LOGIC — strip whitespace from all string columns to prevent silent validation failures
    for col in raw_df.columns:
        raw_df[col] = raw_df[col].where(raw_df[col].isna(), raw_df[col].str.strip())

    # LOGIC — warn if expected columns are missing, but do not fail here;
    # per-row validation in row_validator.py will handle missing column values
    missing_cols = [c for c in _EXPECTED_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        logger.warning(
            "CSV is missing expected columns: %s — validation will reject all affected rows",
            missing_cols,
        )

    return raw_df, desk_code, trade_date


def _validate_filename(filename: str) -> tuple:
    # LOGIC — validates filename against exact pattern; raises ValueError with descriptive message on mismatch
    match = _FILENAME_PATTERN.match(filename)
    if not match:
        raise ValueError(
            f"Filename '{filename}' does not match expected pattern "
            f"'{{desk_code}}_{{YYYY-MM-DD}}_positions.csv'. "
            f"Pattern: ^([A-Za-z0-9]+)_(\\d{{4}}-\\d{{2}}-\\d{{2}})_positions\\.csv$"
        )

    desk_code = match.group(1)
    trade_date_str = match.group(2)

    # LOGIC — parse trade_date string to datetime.date; raises ValueError on invalid date
    try:
        trade_date = datetime.date.fromisoformat(trade_date_str)
    except ValueError as exc:
        raise ValueError(
            f"Filename '{filename}' contains an invalid trade_date '{trade_date_str}': {exc}"
        ) from exc

    logger.debug("Filename parsed: desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date