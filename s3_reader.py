# BOILERPLATE
import io
import logging
import re

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — filename pattern as specified in the design
_FILENAME_PATTERN = re.compile(r".*/([^/]+)_(\d{8})_positions\.csv$")


def list_incoming_files(bucket: str, prefix: str) -> list:
    # LOGIC
    # Returns list of S3 object keys matching pattern:
    #   {prefix}{desk_code}_{trade_date}_positions.csv
    # Uses paginator to handle buckets with many objects.
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator("list_objects_v2")

    matching_keys = []
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if _FILENAME_PATTERN.match(key):
                matching_keys.append(key)
                logger.info("Found incoming file: s3://%s/%s", bucket, key)
            else:
                logger.debug(
                    "Skipping key that does not match position file pattern: %s", key
                )

    logger.info(
        "Listed %d matching files under s3://%s/%s",
        len(matching_keys),
        bucket,
        prefix,
    )
    return matching_keys


def read_csv_from_s3(bucket: str, key: str) -> tuple:
    # LOGIC
    # Returns (raw_dataframe, desk_code, trade_date) parsed from filename.
    # Raises ValueError if filename does not match the expected pattern.

    match = _FILENAME_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            "'<prefix>/<desk_code>_<YYYYMMDD>_positions.csv'"
        )

    desk_code = match.group(1)
    trade_date = match.group(2)

    logger.info(
        "Reading CSV from s3://%s/%s (desk_code=%s, trade_date=%s)",
        bucket,
        key,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — download object bytes, decode in-memory (no /tmp/)
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()

    # LOGIC — parse CSV; all columns read as object initially to preserve raw values
    # for validation; numeric casting happens in validator.py
    raw_df = pd.read_csv(
        io.BytesIO(raw_bytes),
        dtype=str,          # keep all values as strings for raw validation
        keep_default_na=False,  # do not silently convert empty strings to NaN
    )

    logger.info(
        "Read %d rows from s3://%s/%s",
        len(raw_df),
        bucket,
        key,
    )

    # LOGIC — strip leading/trailing whitespace from column names to guard
    # against accidental whitespace in header row
    raw_df.columns = [col.strip() for col in raw_df.columns]

    return raw_df, desk_code, trade_date