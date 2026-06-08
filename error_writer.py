# BOILERPLATE
import os
import re
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — Eastern Time timezone constant
_ET = pytz.timezone("America/Toronto")

# LOGIC — regex to strip .csv extension from source filename if present
_CSV_EXTENSION_RE = re.compile(r"\.csv$", re.IGNORECASE)


def _get_et_timestamp() -> str:
    # LOGIC — current time in America/Toronto formatted as yyyymmddHHMMSS
    now_et = datetime.now(_ET)
    return now_et.strftime("%Y%m%d%H%M%S")


def _build_error_key(source_filename: str, timestamp: str) -> str:
    # LOGIC — construct S3 error key matching pattern:
    # errors/{desk_code}_{trade_date}_positions_errors_{yyyymmddHHMMSS}.csv
    # Strip .csv extension from source_filename if present, then append suffix.
    base = _CSV_EXTENSION_RE.sub("", source_filename)
    return f"errors/{base}_errors_{timestamp}.csv"


def write_error_file(
    rejected_df: pd.DataFrame,
    source_filename: str,
    bucket: str,
) -> str | None:
    # LOGIC — return None immediately if there are no rejected rows; do not write
    if rejected_df is None or rejected_df.empty:
        logger.info("No rejected rows; skipping error file write.")
        return None

    timestamp = _get_et_timestamp()

    # LOGIC — source_filename may be a full path like "incoming/DESK_2026-06-01_positions.csv"
    # Extract only the basename for the error key construction
    basename = os.path.basename(source_filename)
    error_key = _build_error_key(basename, timestamp)

    # LOGIC — serialise rejected rows (all original columns + rejection_reason) to CSV bytes
    csv_bytes = rejected_df.to_csv(index=False).encode("utf-8")

    # BOILERPLATE — upload to S3 using existing bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Wrote %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        error_key,
    )

    return error_key