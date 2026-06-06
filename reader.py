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
# desk_code: one or more word characters; trade_date: YYYY-MM-DD
_FILENAME_RE = re.compile(
    r"^(?P<desk_code>[A-Za-z0-9_-]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# LOGIC — mandatory columns expected in every input CSV
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _parse_filename(key: str) -> tuple[str, str]:
    """
    Extract desk_code and trade_date from the S3 object key.
    The basename of the key must match the pattern
    {desk_code}_{trade_date}_positions.csv.

    Raises ValueError if the pattern does not match.
    """
    # LOGIC
    file_name = os.path.basename(key)
    match = _FILENAME_RE.match(file_name)
    if not match:
        raise ValueError(
            f"Filename '{file_name}' does not match the required pattern "
            "'<desk_code>_<YYYY-MM-DD>_positions.csv'. "
            "Check that desk_code contains only alphanumeric characters, "
            "underscores, or hyphens, and that trade_date is in YYYY-MM-DD format."
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    logger.info("Parsed filename desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def download_and_parse(bucket: str, key: str) -> tuple[pd.DataFrame, int, str, str]:
    """
    Download the CSV file from S3 and parse it into a DataFrame.

    All columns are read as strings (dtype=str) to prevent silent type coercion.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    key : str
        S3 object key.

    Returns
    -------
    tuple of (raw_df, row_count, desk_code, trade_date)
        raw_df       : pd.DataFrame — all columns as strings
        row_count    : int          — number of data rows (excluding header)
        desk_code    : str          — extracted from filename
        trade_date   : str          — extracted from filename (YYYY-MM-DD)

    Raises
    ------
    ValueError
        If the filename does not match the expected pattern, or if mandatory
        columns are missing from the CSV.
    """
    # LOGIC — validate filename before incurring S3 download cost
    desk_code, trade_date = _parse_filename(key)

    # BOILERPLATE — construct S3 client and download object bytes
    s3_client = boto3.client("s3")
    logger.info("Downloading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()
    logger.info("Downloaded %d bytes", len(body_bytes))

    # LOGIC — parse CSV with all columns as strings; use BytesIO to avoid disk I/O
    raw_df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,  # prevent pandas from converting empty strings to NaN silently
    )

    # LOGIC — strip whitespace from column names to tolerate minor formatting variation
    raw_df.columns = [col.strip() for col in raw_df.columns]

    # LOGIC — verify all expected columns are present
    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in raw_df.columns]
    if missing_columns:
        raise ValueError(
            f"CSV is missing required columns: {missing_columns}. "
            f"Found columns: {list(raw_df.columns)}"
        )

    # LOGIC — strip leading/trailing whitespace from all cell values
    for col in _EXPECTED_COLUMNS:
        raw_df[col] = raw_df[col].str.strip()

    row_count = len(raw_df)
    logger.info(
        "Parsed CSV rows=%d columns=%s",
        row_count,
        list(raw_df.columns),
    )

    return raw_df, row_count, desk_code, trade_date