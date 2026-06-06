import io
import logging
import os

import boto3
import pandas as pd

from exceptions import FileReadError  # BOILERPLATE

# BOILERPLATE
logger = logging.getLogger(__name__)


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    # BOILERPLATE — establish S3 client using ambient IAM credentials; no hardcoded credentials
    s3_client = boto3.client("s3")

    # LOGIC — download the S3 object and raise a typed exception on any failure
    try:
        logger.info("Fetching s3://%s/%s", bucket, key)
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        logger.error("Failed to retrieve s3://%s/%s: %s", bucket, key, exc)
        raise FileReadError(
            f"Unable to retrieve s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — parse CSV with all columns as strings; no type coercion at read time
    try:
        body_bytes = response["Body"].read()
        df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,          # all columns remain str — coercion is the validator's job
            keep_default_na=False,  # do not silently convert "NA", "NaN", "" to float NaN
            na_values=[""],     # only treat empty string as NaN so we can detect missing fields
        )
    except Exception as exc:
        logger.error("Failed to parse CSV from s3://%s/%s: %s", bucket, key, exc)
        raise FileReadError(
            f"Unable to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    logger.info(
        "Read %d rows and %d columns from s3://%s/%s",
        len(df),
        len(df.columns),
        bucket,
        key,
    )
    return df