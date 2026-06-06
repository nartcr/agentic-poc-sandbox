# BOILERPLATE
import io
import logging
import os
import re

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_filename(key: str) -> tuple:
    """
    # LOGIC
    Extract desk_code and trade_date string from an S3 key.
    Only the basename (final path component) is matched against the pattern.
    Raises ValueError if the filename does not match the expected pattern.
    """
    basename = os.path.basename(key)
    match = _FILENAME_PATTERN.match(basename)
    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'. "
            f"Full key: '{key}'"
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    logger.debug(
        "Parsed filename '%s': desk_code='%s', trade_date='%s'",
        basename,
        desk_code,
        trade_date,
    )
    return desk_code, trade_date


def read_csv_from_s3(bucket: str, key: str) -> tuple:
    """
    # LOGIC
    Download a CSV file from S3 and return a raw DataFrame plus file metadata.

    Returns:
        (DataFrame, metadata_dict) where:
          - DataFrame has all columns as object dtype (no type coercion)
          - metadata_dict keys: source_file, row_count_raw,
            desk_code_from_filename, trade_date_from_filename
    """
    # BOILERPLATE
    logger.info("Reading CSV from s3://%s/%s", bucket, key)
    s3_client = boto3.client("s3")

    # LOGIC — fetch object body from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()
    logger.debug(
        "Downloaded %d bytes from s3://%s/%s", len(body_bytes), bucket, key
    )

    # LOGIC — parse CSV with all columns as object dtype; disable default NA
    # conversion so validation rules see the raw string values, not NaN
    df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,
        na_values=[],
    )
    logger.info(
        "Parsed CSV from s3://%s/%s: %d rows, %d columns",
        bucket,
        key,
        len(df),
        len(df.columns),
    )

    # LOGIC — parse desk_code and trade_date from filename
    desk_code_from_filename, trade_date_from_filename = _parse_filename(key)

    # LOGIC — build metadata dict per spec
    metadata = {
        "source_file": key,
        "row_count_raw": len(df),
        "desk_code_from_filename": desk_code_from_filename,
        "trade_date_from_filename": trade_date_from_filename,
    }
    logger.debug("File metadata: %s", metadata)

    return df, metadata