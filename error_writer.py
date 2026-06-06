# BOILERPLATE
import io
import logging

import boto3
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


# LOGIC
def write_error_file(
    bucket: str,
    error_prefix: str,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
) -> str:
    """
    Serializes rejected_df to CSV (UTF-8 with BOM for Excel compatibility).
    Uploads to s3://{bucket}/{error_prefix}{desk_code}_{trade_date}_positions_errors_{ts}.csv
    Returns the full S3 key of the written error file.
    Does nothing (returns empty string) if rejected_df is empty.
    """
    # LOGIC — guard: nothing to write if no rejected rows
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return ""

    # LOGIC — format the ET processing timestamp as YYYYMMDDTHHMMSS
    ts_str = processing_timestamp.strftime("%Y%m%dT%H%M%S")

    # LOGIC — construct the S3 key using the specified pattern
    s3_key = (
        f"{error_prefix}{desk_code}_{trade_date}_positions_errors_{ts_str}.csv"
    )

    # LOGIC — serialize DataFrame to CSV bytes with UTF-8 BOM (utf-8-sig) for Excel compatibility
    csv_buffer = io.BytesIO()
    rejected_df.to_csv(
        csv_buffer,
        index=False,
        encoding="utf-8-sig",
    )
    csv_bytes = csv_buffer.getvalue()

    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows, %d bytes).",
        bucket,
        s3_key,
        len(rejected_df),
        len(csv_bytes),
    )

    # BOILERPLATE — upload to S3 using existing bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )

    logger.info("Error file written successfully: s3://%s/%s", bucket, s3_key)
    return s3_key