# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    bucket: str,
) -> str | None:
    # LOGIC — short-circuit when there are no rejected rows
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s — skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — build the deterministic S3 key for the error file
    s3_key = f"errors/{desk_code}_{trade_date}_positions_errors.csv"

    # LOGIC — serialize rejected DataFrame to CSV in memory; no filesystem writes
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — obtain S3 client and upload
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written: s3://%s/%s (%d rejected rows).",
        bucket,
        s3_key,
        len(rejected_df),
    )

    # LOGIC — return the full S3 key written so the caller can record it
    return s3_key