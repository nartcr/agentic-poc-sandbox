# BOILERPLATE
import io
import re
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — S3 key pattern per data contract: incoming/{desk_code}_{trade_date}_positions.csv
_KEY_PATTERN = re.compile(
    r"^incoming/([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def download_and_parse(s3_client, bucket: str, s3_key: str) -> tuple:
    """
    Downloads a CSV from S3 and parses it into a raw DataFrame with all columns as strings.

    Returns:
        (dataframe, total_row_count)

    Raises:
        ValueError: if the file is empty or cannot be parsed.
    """
    # LOGIC — download object bytes from S3
    logger.info("Downloading s3://%s/%s", bucket, s3_key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        body_bytes = response["Body"].read()
    except Exception as exc:
        logger.error("Failed to download s3://%s/%s: %s", bucket, s3_key, exc)
        raise

    # LOGIC — parse CSV; dtype=str preserves all values as raw strings;
    #          keep_default_na=False prevents blank cells becoming NaN silently
    try:
        dataframe = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,
            keep_default_na=False,
        )
    except Exception as exc:
        logger.error("Failed to parse CSV from s3://%s/%s: %s", bucket, s3_key, exc)
        raise ValueError(f"CSV parse error for key '{s3_key}': {exc}") from exc

    # LOGIC — reject empty files (zero data rows after header)
    total_row_count = len(dataframe)
    if total_row_count == 0:
        raise ValueError(
            f"File at s3://{bucket}/{s3_key} is empty (zero data rows after header)."
        )

    logger.info(
        "Parsed %d rows from s3://%s/%s", total_row_count, bucket, s3_key
    )
    return dataframe, total_row_count


def extract_metadata_from_key(s3_key: str) -> tuple:
    """
    Extracts desk_code and trade_date from the S3 key.

    Expected pattern: incoming/{desk_code}_{trade_date}_positions.csv

    Returns:
        (desk_code, trade_date) as strings

    Raises:
        ValueError: if the key does not match the expected pattern.
    """
    # LOGIC — validate key format and extract named groups
    match = _KEY_PATTERN.match(s3_key)
    if not match:
        raise ValueError(
            f"S3 key '{s3_key}' does not match expected pattern "
            r"'^incoming/([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$'."
        )

    desk_code = match.group(1)
    trade_date = match.group(2)

    logger.info(
        "Extracted metadata from key — desk_code='%s', trade_date='%s'",
        desk_code,
        trade_date,
    )
    return desk_code, trade_date