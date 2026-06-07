# BOILERPLATE
import io
import logging
import os
import re
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — filename pattern that all valid input files must match
_FILENAME_PATTERN = re.compile(
    r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def list_input_files(s3_client, config) -> list:
    """
    # LOGIC
    Lists all S3 object keys under config.s3_input_prefix that match
    the expected filename pattern: {desk_code}_{trade_date}_positions.csv

    Returns a list of full S3 object key strings.
    Raises RuntimeError if the S3 call fails.
    """
    bucket = config.s3_bucket
    prefix = config.s3_input_prefix

    logger.info(
        "Listing input files in s3://%s/%s", bucket, prefix
    )

    matching_keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = os.path.basename(key)
                # LOGIC — only include keys whose filename matches the pattern
                if _FILENAME_PATTERN.match(filename):
                    matching_keys.append(key)
                    logger.debug("Found matching input file: %s", key)
                else:
                    logger.debug(
                        "Skipping non-matching key: %s", key
                    )
    except Exception as exc:
        logger.error(
            "Failed to list objects in s3://%s/%s: %s",
            bucket, prefix, exc
        )
        raise RuntimeError(
            f"Failed to list input files from s3://{bucket}/{prefix}: {exc}"
        ) from exc

    logger.info("Found %d matching input file(s).", len(matching_keys))
    return matching_keys


def read_position_file(s3_client, bucket: str, s3_key: str) -> pd.DataFrame:
    """
    # LOGIC
    Downloads a CSV file from S3 and returns it as a raw pandas DataFrame.
    All columns are read as strings with NA fill disabled so that
    downstream validation can apply its own type-coercion and null checks.
    Column names are preserved exactly as found in the file header.

    Raises RuntimeError if the S3 GET fails.
    Raises ValueError if the downloaded object cannot be parsed as CSV.
    """
    logger.info("Reading position file from s3://%s/%s", bucket, s3_key)

    try:
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        body_bytes = response["Body"].read()
    except Exception as exc:
        logger.error(
            "Failed to GET s3://%s/%s: %s", bucket, s3_key, exc
        )
        raise RuntimeError(
            f"Failed to read s3://{bucket}/{s3_key}: {exc}"
        ) from exc

    try:
        # LOGIC — dtype=str and keep_default_na=False preserve raw string values;
        # no silent coercion of "NA", "None", empty string to float NaN here.
        raw_df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,
            keep_default_na=False,
        )
    except Exception as exc:
        logger.error(
            "Failed to parse CSV from s3://%s/%s: %s", bucket, s3_key, exc
        )
        raise ValueError(
            f"Failed to parse CSV content from s3://{bucket}/{s3_key}: {exc}"
        ) from exc

    logger.info(
        "Read %d row(s) and %d column(s) from %s.",
        len(raw_df), len(raw_df.columns), s3_key,
    )
    return raw_df


def parse_filename_metadata(s3_key: str) -> tuple:
    """
    # LOGIC
    Extracts desk_code (str) and trade_date (datetime.date) from the
    filename embedded in an S3 key.

    Expected filename format: {DESK_CODE}_{YYYY-MM-DD}_positions.csv
    The S3 key may include a prefix path; only the basename is matched.

    Returns: (desk_code: str, trade_date: datetime.date)
    Raises ValueError if the filename does not match the expected pattern.
    """
    filename = os.path.basename(s3_key)
    match = _FILENAME_PATTERN.match(filename)

    if not match:
        logger.error(
            "Filename '%s' (from key '%s') does not match expected pattern.",
            filename, s3_key,
        )
        raise ValueError(
            f"Filename '{filename}' does not match expected pattern "
            f"'<DESK_CODE>_<YYYY-MM-DD>_positions.csv'. S3 key: '{s3_key}'"
        )

    desk_code = match.group(1)
    trade_date_str = match.group(2)

    # LOGIC — parse the date string; ValueError propagates if somehow malformed
    # (the regex already ensures the format, but explicit parsing is safer)
    from datetime import datetime  # BOILERPLATE — local import to avoid cycle risk
    trade_date = datetime.strptime(trade_date_str, "%Y-%m-%d").date()

    logger.info(
        "Parsed filename metadata: desk_code='%s', trade_date=%s",
        desk_code, trade_date,
    )
    return desk_code, trade_date