# BOILERPLATE
import logging
import os
import io

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC
REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_position_file(bucket: str, key: str) -> pd.DataFrame:
    # LOGIC — Download CSV from S3, parse into DataFrame, enforce required columns
    logger.info("Reading position file from s3://%s/%s", bucket, key)

    # BOILERPLATE — S3 client and object retrieval
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()

    # LOGIC — Parse CSV from in-memory bytes
    df = pd.read_csv(io.BytesIO(raw_bytes))

    # LOGIC — Strip whitespace from all column headers
    df.columns = [col.strip() for col in df.columns]

    # LOGIC — Validate all required columns are present
    present_columns = set(df.columns)
    missing = [col for col in REQUIRED_COLUMNS if col not in present_columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    logger.info(
        "Successfully read %d rows from s3://%s/%s",
        len(df),
        bucket,
        key,
    )
    return df