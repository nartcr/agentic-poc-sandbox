# BOILERPLATE
import io
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)


import re

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_desk_and_date_from_key(source_file_key: str):
    """
    Extracts desk_code and trade_date string from an S3 key whose basename
    follows the pattern {desk_code}_{trade_date}_positions.csv.
    Handles multi-segment desk codes (e.g. FX_SPOT).
    Returns (desk_code, trade_date_str).
    """
    # LOGIC: derive basename from the S3 key (last path component)
    basename = os.path.basename(source_file_key)
    match = _FILENAME_PATTERN.match(basename)
    if not match:
        raise ValueError(
            f"Cannot parse desk_code/trade_date from source_file_key: {source_file_key!r}. "
            f"Expected pattern {{desk_code}}_{{trade_date}}_positions.csv."
        )
    return match.group("desk_code"), match.group("trade_date")


def write_error_file(
    bucket: str,
    errors_prefix: str,
    rejected_df: pd.DataFrame,
    source_file_key: str,
) -> str:
    """
    Writes the rejected rows DataFrame to an S3 CSV error file.
    Returns the full S3 key of the written error file, or None if there are no rejected rows.
    """
    # LOGIC: if there are no rejections, do not write any file
    if rejected_df is None or rejected_df.empty:
        logger.info(
            "No rejected rows for source_file=%s; skipping error file write.", source_file_key
        )
        return None

    # LOGIC: derive desk_code and trade_date from the source file key
    desk_code, trade_date_str = _parse_desk_and_date_from_key(source_file_key)

    # LOGIC: compute run timestamp in ET for the filename
    et_tz = pytz.timezone("America/Toronto")
    run_timestamp = datetime.now(et_tz).strftime("%Y%m%d_%H%M%S")

    # LOGIC: build the error file S3 key
    error_filename = f"{desk_code}_{trade_date_str}_positions_errors_{run_timestamp}.csv"
    error_s3_key = f"{errors_prefix}{error_filename}"

    # LOGIC: serialize the rejected DataFrame to CSV in memory (no local filesystem)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE: upload to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=error_s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Wrote error file for source_file=%s; rejected_rows=%d; s3_key=%s",
        source_file_key,
        len(rejected_df),
        error_s3_key,
    )
    return error_s3_key