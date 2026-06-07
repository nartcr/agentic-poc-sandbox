# BOILERPLATE
import io
import logging
import os
import re

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename regex: {desk_code}_{trade_date}_positions.csv
# desk_code is one or more uppercase alphanumeric chars
# trade_date is YYYY-MM-DD
# The pattern is anchored so partial matches are rejected
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>[A-Z0-9]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# BOILERPLATE — expected CSV columns per data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def parse_filename(filename: str) -> dict:
    """
    Parses a filename conforming to {desk_code}_{trade_date}_positions.csv.

    Returns {"desk_code": str, "trade_date": str, "filename": str}.
    Raises ValueError if the filename does not match the expected pattern.
    """
    # LOGIC — strip any S3 key prefix; operate only on the basename
    basename = filename.split("/")[-1]

    match = re.match(_FILENAME_PATTERN, basename)  # LOGIC — regex, never split('_')
    if match is None:
        raise ValueError(
            f"Filename '{basename}' does not match the required pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv' "
            f"(desk_code must be [A-Z0-9]+, trade_date must be YYYY-MM-DD)."
        )

    desk_code: str = match.group("desk_code")
    trade_date: str = match.group("trade_date")

    logger.info(
        "Parsed filename: basename=%s desk_code=%s trade_date=%s",
        basename,
        desk_code,
        trade_date,
    )

    return {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "filename": basename,
    }


def download_and_parse(bucket: str, object_key: str) -> tuple:
    """
    Downloads the CSV file from S3 into memory (no /tmp/ writes).
    Parses the filename to extract metadata.
    Returns (pd.DataFrame, dict) where dict contains desk_code, trade_date, filename.

    Raises ValueError if the filename pattern does not match.
    Raises RuntimeError if the S3 object cannot be read or the CSV is malformed.
    """
    # LOGIC — parse filename first so we fail fast before downloading
    metadata: dict = parse_filename(object_key)

    # BOILERPLATE — build S3 client; credentials come from the Lambda execution role
    s3_client = boto3.client("s3")

    logger.info(
        "Downloading s3://%s/%s into memory", bucket, object_key
    )

    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        raw_bytes: bytes = response["Body"].read()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to download s3://{bucket}/{object_key}: {exc}"
        ) from exc

    # LOGIC — parse CSV into DataFrame entirely in memory via BytesIO
    try:
        df: pd.DataFrame = pd.read_csv(
            io.BytesIO(raw_bytes),
            dtype=str,          # read all columns as str to preserve raw values for validation
            keep_default_na=False,  # do not silently convert empty strings to NaN
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to parse CSV from s3://{bucket}/{object_key}: {exc}"
        ) from exc

    # LOGIC — replace empty strings with pd.NA so validation can detect missing values
    # pandas read_csv with keep_default_na=False returns "" for blank cells;
    # convert "" → pd.NA so downstream null checks work uniformly
    df = df.replace("", pd.NA)

    logger.info(
        "Downloaded and parsed: filename=%s rows=%d columns=%s",
        metadata["filename"],
        len(df),
        list(df.columns),
    )

    return df, metadata