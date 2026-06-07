# BOILERPLATE
import io
import json
import logging

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    bucket: str,
) -> "str | None":
    """
    Write rejected rows to S3 as a CSV under the errors/ prefix.

    Returns the S3 key written, or None if rejected_df is empty.
    """
    # LOGIC — early exit when there are no rejected rows
    if rejected_df is None or rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s — error file not written.",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — build the S3 key per the DATA CONTRACT
    s3_key = f"errors/{desk_code}_{trade_date}_positions_errors.csv"

    # LOGIC — serialise DataFrame to CSV in memory (no filesystem writes)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — obtain S3 client and upload
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written: s3://%s/%s  rows=%d",
        bucket,
        s3_key,
        len(rejected_df),
    )
    return s3_key