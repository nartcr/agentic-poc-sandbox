# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

from pipeline_exceptions import FileReadError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — expected columns as defined in the data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


# LOGIC
def download_and_parse(bucket: str, key: str) -> pd.DataFrame:
    """Download a CSV file from S3 into memory and parse it into a DataFrame.

    All columns are returned as Python strings (dtype=str) to preserve exact
    input values for downstream validation.  The DataFrame index is set to the
    1-based line number in the original file (header = line 1, first data row
    = line 2, etc.) so that error reports can reference the source line.

    Args:
        bucket: S3 bucket name.
        key:    S3 object key (e.g. ``incoming/DESK1_2026-01-15_positions.csv``).

    Returns:
        pd.DataFrame with all columns as ``str``, index starting at 2.

    Raises:
        FileReadError: if the S3 object cannot be retrieved, the CSV cannot be
                       parsed, any expected column is absent, or the file
                       contains no data rows.
    """
    # LOGIC — create S3 client; bucket name comes from the caller (S3 event),
    #          not hardcoded here.  The env var is the canonical source for the
    #          bucket used by other modules, but this function accepts the
    #          bucket explicitly so the handler can pass the event-supplied
    #          value, ensuring we always operate on the correct object.
    s3_client = boto3.client("s3")

    logger.info("Downloading s3://%s/%s", bucket, key)

    # LOGIC — download into memory; no /tmp/ path used
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw_bytes: bytes = response["Body"].read()
    except Exception as exc:
        raise FileReadError(
            f"Failed to download s3://{bucket}/{key}: {exc}"
        ) from exc

    if not raw_bytes:
        raise FileReadError(
            f"Downloaded object is empty: s3://{bucket}/{key}"
        )

    logger.info("Downloaded %d bytes from s3://%s/%s", len(raw_bytes), bucket, key)

    # LOGIC — parse CSV from bytes in memory; all values kept as str so that
    #          validation rules can inspect the raw text exactly as received.
    try:
        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            dtype=str,          # preserve exact input values
            keep_default_na=False,  # do not silently convert "" to NaN
            encoding="utf-8",
        )
    except Exception as exc:
        raise FileReadError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — reject completely empty files (header only, no data rows)
    if df.empty:
        raise FileReadError(
            f"CSV file contains no data rows: s3://{bucket}/{key}"
        )

    # LOGIC — validate that all expected columns are present (extra columns
    #          are permitted and ignored by downstream modules, but the
    #          mandatory columns must exist so validation can proceed).
    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise FileReadError(
            f"CSV is missing required columns {missing_columns} "
            f"in s3://{bucket}/{key}. Found columns: {list(df.columns)}"
        )

    # LOGIC — set index to 1-based line numbers in the original file.
    #          Header is line 1; first data row is line 2; hence range(2, n+2).
    n = len(df)
    df.index = range(2, n + 2)

    logger.info(
        "Parsed CSV: %d data rows, columns=%s", n, list(df.columns)
    )

    return df