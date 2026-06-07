# BOILERPLATE
import io
import logging
import os
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def list_pending_files(s3_client, bucket: str, prefix: str) -> list:
    # LOGIC
    # Lists all S3 object keys under the given prefix that end with '_positions.csv'.
    # Uses paginator to handle buckets with more than 1000 objects.
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    matching_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("_positions.csv"):
                matching_keys.append(key)
                logger.info("Discovered pending file: s3://%s/%s", bucket, key)

    logger.info(
        "Found %d pending position file(s) under s3://%s/%s",
        len(matching_keys),
        bucket,
        prefix,
    )
    return matching_keys


def read_position_file(s3_client, bucket: str, key: str):
    # LOGIC
    # Extracts desk_code and trade_date from the filename, then reads the CSV into a DataFrame.
    # Expected filename pattern: {desk_code}_{trade_date}_positions.csv
    # trade_date in filename is YYYYMMDD; returned as YYYY-MM-DD string.

    basename = os.path.basename(key)

    # LOGIC — validate and parse filename pattern
    if not basename.endswith("_positions.csv"):
        raise ValueError(
            f"File '{basename}' does not end with '_positions.csv'. "
            "Expected pattern: {{desk_code}}_{{trade_date}}_positions.csv"
        )

    # Strip the trailing '_positions.csv' suffix
    stem = basename[: -len("_positions.csv")]  # e.g. "EQDESK_20260610"

    # LOGIC — split on the last '_' to separate desk_code from trade_date
    # This supports desk codes that may contain underscores (e.g. "EQ_DESK_20260610")
    # by splitting only on the final underscore-separated 8-digit segment.
    last_underscore = stem.rfind("_")
    if last_underscore == -1:
        raise ValueError(
            f"Filename stem '{stem}' does not contain '_' separator between "
            "desk_code and trade_date. Expected pattern: {{desk_code}}_{{YYYYMMDD}}_positions.csv"
        )

    desk_code = stem[:last_underscore]
    trade_date_raw = stem[last_underscore + 1 :]

    if not desk_code:
        raise ValueError(
            f"Could not extract desk_code from filename '{basename}'. "
            "Expected pattern: {{desk_code}}_{{YYYYMMDD}}_positions.csv"
        )

    # LOGIC — validate trade_date is a parseable YYYYMMDD date
    try:
        parsed_date = datetime.strptime(trade_date_raw, "%Y%m%d")
    except ValueError:
        raise ValueError(
            f"trade_date segment '{trade_date_raw}' in filename '{basename}' "
            "is not a valid date in YYYYMMDD format."
        )

    trade_date = parsed_date.strftime("%Y-%m-%d")

    logger.info(
        "Parsed filename '%s': desk_code='%s', trade_date='%s'",
        basename,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — read CSV from S3 into memory
    logger.info("Reading S3 object: s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    # LOGIC — parse CSV into DataFrame; keep all columns present in the file
    raw_df = pd.read_csv(io.BytesIO(body_bytes), dtype=str)

    logger.info(
        "Read %d rows from s3://%s/%s (columns: %s)",
        len(raw_df),
        bucket,
        key,
        list(raw_df.columns),
    )

    return raw_df, desk_code, trade_date