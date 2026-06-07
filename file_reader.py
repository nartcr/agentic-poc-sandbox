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

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# desk_code: alphanumeric only; trade_date: YYYY-MM-DD; suffix fixed
_FILENAME_PATTERN = re.compile(
    r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# LOGIC — all seven mandatory columns expected in the CSV
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_csv_from_s3(bucket: str, key: str) -> tuple:
    """
    Fetches the CSV object at s3://bucket/key, parses it into a DataFrame,
    and extracts desk_code and trade_date from the filename portion of the key.

    Returns:
        (dataframe, desk_code, trade_date_str)

    Raises:
        ValueError: if the filename does not match the expected pattern.
        botocore.exceptions.ClientError: if the S3 object cannot be fetched.
    """
    # LOGIC — extract the bare filename from the full S3 key (e.g. "incoming/DESK_2024-01-15_positions.csv")
    filename = _extract_filename(key)
    logger.info("Parsing filename. key=%s filename=%s", key, filename)

    # LOGIC — validate and extract metadata from the filename using regex (never str.split)
    desk_code, trade_date_str = _parse_filename(filename)
    logger.info(
        "Filename parsed successfully. desk_code=%s trade_date=%s",
        desk_code,
        trade_date_str,
    )

    # BOILERPLATE — fetch object from S3
    s3_client = boto3.client("s3")
    logger.info("Fetching S3 object. bucket=%s key=%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)

    # LOGIC — read body as UTF-8 text and parse into DataFrame
    raw_bytes = response["Body"].read()
    csv_text = raw_bytes.decode("utf-8")
    dataframe = pd.read_csv(io.StringIO(csv_text))

    logger.info(
        "CSV parsed. rows=%d columns=%s",
        len(dataframe),
        list(dataframe.columns),
    )

    return dataframe, desk_code, trade_date_str


def _extract_filename(key: str) -> str:
    """
    Returns the final path component of an S3 key.
    e.g. 'incoming/DESK_2024-01-15_positions.csv' → 'DESK_2024-01-15_positions.csv'
    """
    # LOGIC — use posix-style split on '/' to get basename; avoids os.path platform issues
    parts = key.rstrip("/").split("/")
    return parts[-1]


def _parse_filename(filename: str) -> tuple:
    """
    Applies the mandatory filename regex to extract desk_code and trade_date_str.

    Pattern: ^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\\.csv$

    Returns:
        (desk_code: str, trade_date_str: str)

    Raises:
        ValueError: if the filename does not match the expected pattern.
    """
    # LOGIC — match against strict pattern; reject any filename that doesn't conform
    match = _FILENAME_PATTERN.match(filename)
    if match is None:
        raise ValueError(
            f"Filename '{filename}' does not match required pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv' "
            f"(desk_code must be alphanumeric, trade_date must be YYYY-MM-DD)."
        )

    desk_code = match.group(1)
    trade_date_str = match.group(2)

    return desk_code, trade_date_str