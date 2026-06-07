# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date_str: str,
) -> str:
    # LOGIC — construct the S3 key for the error file
    s3_key = f"errors/{desk_code}_{trade_date_str}_errors.csv"

    # LOGIC — serialize rejected_df to CSV bytes (always write, even if empty)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )

    logger.info(
        "Wrote error file to s3://%s/%s (%d rejected rows)",
        bucket,
        s3_key,
        len(rejected_df),
    )

    return s3_key