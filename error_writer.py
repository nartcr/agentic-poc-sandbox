# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


# LOGIC
def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date_str: str,
    timestamp_et: datetime,
) -> "str | None":
    """Write rejected rows as a CSV to S3 under the errors/ prefix.

    Returns the S3 key of the written error file, or None if rejected_df is empty.
    The timestamp_et argument must be an ET-aware datetime (America/Toronto).
    """
    # LOGIC — guard: nothing to write when there are no rejections
    if rejected_df is None or rejected_df.empty:
        logger.info(
            "No rejected rows; skipping error file write for desk_code=%s trade_date=%s",
            desk_code,
            trade_date_str,
        )
        return None

    # LOGIC — build the S3 key using the exact pattern from the design
    timestamp_str = timestamp_et.strftime("%Y%m%dT%H%M%S")
    s3_key = f"errors/{desk_code}_{trade_date_str}_positions_errors_{timestamp_str}.csv"

    # LOGIC — serialise the DataFrame to CSV in memory (UTF-8, with header)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — build S3 client and upload
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
        ContentEncoding="utf-8",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows)",
        bucket,
        s3_key,
        len(rejected_df),
    )
    return s3_key