# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    # LOGIC — download S3 object and parse as CSV; all columns kept as raw strings
    logger.info("Reading CSV from s3://%s/%s", bucket, key)

    s3_client = boto3.client("s3")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    if not body_bytes.strip():
        # LOGIC — empty file: log warning and return empty DataFrame
        logger.warning("S3 object s3://%s/%s is empty; returning empty DataFrame", bucket, key)
        return pd.DataFrame()

    # LOGIC — parse CSV treating every column as str to preserve raw values for validation
    raw_df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,   # LOGIC — prevent pandas from silently converting "" to NaN
    )

    if raw_df.empty:
        logger.warning(
            "S3 object s3://%s/%s produced an empty DataFrame (header only or no rows)",
            bucket,
            key,
        )
    else:
        logger.info(
            "Read %d rows and %d columns from s3://%s/%s",
            len(raw_df),
            len(raw_df.columns),
            bucket,
            key,
        )

    return raw_df