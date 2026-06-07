# BOILERPLATE
import io
import logging
import re

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern enforced by the data contract
_FILENAME_PATTERN = re.compile(
    r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# LOGIC — mandatory columns that must be present in the CSV header
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


# LOGIC
def _extract_filename_metadata(key: str) -> dict:
    """
    Parse desk_code and trade_date from a key such as
    incoming/EQUITYDESK1_2026-06-15_positions.csv.

    Only the bare filename (after the last '/') is matched against the
    pattern.  Raises ValueError if the filename does not conform.
    """
    bare_filename = key.split("/")[-1]
    match = _FILENAME_PATTERN.match(bare_filename)
    if match is None:
        raise ValueError(
            f"Filename '{bare_filename}' does not match expected pattern "
            r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    logger.info(
        "Filename parsed. desk_code=%s trade_date=%s", desk_code, trade_date
    )
    return {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "filename": bare_filename,
    }


# LOGIC
def download_and_parse(bucket: str, key: str) -> tuple:
    """
    Download the CSV from S3 and return:
      - a pandas DataFrame with all columns as str dtype
      - a metadata dict with keys: desk_code, trade_date, filename

    Raises:
        ValueError  — if the filename does not match the expected pattern
                      or if a required column is absent from the CSV header.
        Exception   — propagates any S3 or pandas read error.
    """
    # LOGIC — parse filename metadata first (fail fast before any S3 call)
    metadata = _extract_filename_metadata(key)

    # BOILERPLATE — download S3 object into memory
    s3_client = boto3.client("s3")
    logger.info("Downloading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()
    logger.info(
        "Download complete. size_bytes=%d", len(body_bytes)
    )

    # LOGIC — parse CSV; force all columns to str to prevent silent type coercion
    try:
        raw_df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,
            keep_default_na=False,  # keep empty strings as "" rather than NaN
        )
    except Exception as exc:
        raise ValueError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — validate that all required columns are present
    missing_cols = [c for c in _REQUIRED_COLUMNS if c not in raw_df.columns]
    if missing_cols:
        raise ValueError(
            f"CSV is missing required columns: {missing_cols}. "
            f"Found columns: {list(raw_df.columns)}"
        )

    logger.info(
        "CSV parsed successfully. rows=%d columns=%s",
        len(raw_df),
        list(raw_df.columns),
    )

    return raw_df, metadata