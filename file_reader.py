# BOILERPLATE
import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from exceptions import FileReadError

logger = logging.getLogger(__name__)

# LOGIC
def download_and_parse(bucket: str, key: str) -> pd.DataFrame:
    """
    Downloads the CSV object at s3://bucket/key and returns a raw DataFrame
    with all columns typed as str. Column headers are whitespace-stripped.
    Raises FileReadError if the object is missing or cannot be parsed as CSV.
    """
    s3_client = boto3.client("s3")  # BOILERPLATE

    # LOGIC — fetch object from S3
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "S3 get_object failed for s3://%s/%s — error code: %s",
            bucket,
            key,
            error_code,
        )
        raise FileReadError(
            f"S3 object not found or inaccessible: s3://{bucket}/{key} — {error_code}"
        ) from exc

    # LOGIC — read streamed body into memory buffer
    try:
        body_bytes = response["Body"].read()
        buffer = io.BytesIO(body_bytes)
    except Exception as exc:
        logger.error(
            "Failed to read response body for s3://%s/%s: %s", bucket, key, exc
        )
        raise FileReadError(
            f"Failed to read S3 response body for s3://{bucket}/{key}"
        ) from exc

    # LOGIC — parse as CSV with all columns as strings; no type coercion
    try:
        raw_df = pd.read_csv(
            buffer,
            dtype=str,
            keep_default_na=False,  # prevents empty strings becoming NaN
            encoding="utf-8",
            sep=",",
        )
    except Exception as exc:
        logger.error(
            "Failed to parse CSV for s3://%s/%s: %s", bucket, key, exc
        )
        raise FileReadError(
            f"File at s3://{bucket}/{key} is not parseable as CSV: {exc}"
        ) from exc

    # LOGIC — strip whitespace from column headers
    raw_df.columns = [col.strip() for col in raw_df.columns]

    logger.info(
        "Parsed s3://%s/%s — %d rows, %d columns",
        bucket,
        key,
        len(raw_df),
        len(raw_df.columns),
    )

    return raw_df