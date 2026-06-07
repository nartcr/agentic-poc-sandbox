import io
import logging
import re

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — required business columns as specified in data contracts
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# The desk_code may itself contain underscores, so we anchor on the date pattern
_FILENAME_PATTERN = re.compile(
    r"^(.+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_filename(s3_key: str) -> tuple:
    """
    # LOGIC
    Extract desk_code and trade_date from the S3 key filename segment.
    Raises ValueError if the filename does not match the expected pattern.
    """
    filename = s3_key.split("/")[-1]
    match = _FILENAME_PATTERN.match(filename)
    if not match:
        raise ValueError(
            f"Filename '{filename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'. S3 key: {s3_key}"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return desk_code, trade_date


def read_position_file(bucket: str, s3_key: str) -> pd.DataFrame:
    """
    # LOGIC
    Download a single CSV file from S3 and return its contents as a
    pandas DataFrame with all columns read as strings (dtype=str).

    Attaches _source_desk_code and _source_trade_date metadata columns
    derived from the filename.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    s3_key : str
        Full S3 object key, e.g. 'incoming/EQUITIES_2026-06-15_positions.csv'.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame with all original columns as strings plus
        _source_desk_code and _source_trade_date metadata columns.

    Raises
    ------
    ValueError
        If the filename does not match the expected pattern.
    """
    # LOGIC — parse metadata from filename before any S3 call
    desk_code, trade_date = _parse_filename(s3_key)

    # BOILERPLATE — S3 client and object retrieval
    s3_client = boto3.client("s3")
    logger.info("Downloading S3 object: s3://%s/%s", bucket, s3_key)

    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    body_bytes = response["Body"].read()

    # LOGIC — read CSV with all columns as strings to preserve raw values
    df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,  # prevent pandas silently converting "NA" strings
    )

    row_count = len(df)
    logger.info(
        "Read %d rows from s3://%s/%s", row_count, bucket, s3_key
    )

    # LOGIC — attach metadata columns (underscore-prefixed per design convention)
    df["_source_desk_code"] = desk_code
    df["_source_trade_date"] = trade_date

    return df