# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

from ingestion_exceptions import FileReadError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — expected CSV columns per data contract
EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    # LOGIC — download S3 object into memory and parse as CSV with all fields as strings
    logger.info("Reading S3 object s3://%s/%s", bucket, key)

    # BOILERPLATE — boto3 S3 client; credentials from IAM role, never hardcoded
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body_bytes = response["Body"].read()
    except s3_client.exceptions.NoSuchKey:
        logger.error("S3 object not found: s3://%s/%s", bucket, key)
        raise FileReadError(f"S3 object not found: s3://{bucket}/{key}")
    except Exception as exc:
        logger.error(
            "Failed to read S3 object s3://%s/%s: %s", bucket, key, str(exc)
        )
        raise FileReadError(
            f"Failed to read S3 object s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — read CSV entirely in memory (no /tmp/ path); dtype=str preserves all values
    # for downstream validation to catch type errors
    try:
        df = pd.read_csv(io.BytesIO(body_bytes), dtype=str)
    except Exception as exc:
        logger.error(
            "Failed to parse CSV from s3://%s/%s: %s", bucket, key, str(exc)
        )
        raise FileReadError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    logger.info(
        "Successfully read %d rows from s3://%s/%s with columns: %s",
        len(df),
        bucket,
        key,
        list(df.columns),
    )
    return df