# BOILERPLATE
import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.ingestion.exceptions import FileReadError

# BOILERPLATE
logger = logging.getLogger(__name__)


def read_csv_from_s3(s3_bucket: str, s3_key: str) -> pd.DataFrame:
    # BOILERPLATE
    s3_client = boto3.client("s3")

    # LOGIC
    logger.info("Reading S3 object s3://%s/%s", s3_bucket, s3_key)

    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "Failed to retrieve S3 object s3://%s/%s — error code: %s",
            s3_bucket,
            s3_key,
            error_code,
        )
        raise FileReadError(
            f"Cannot retrieve s3://{s3_bucket}/{s3_key}: {error_code}"
        ) from exc

    # LOGIC — read entire body once; parse with dtype=str to preserve raw values
    body_bytes = response["Body"].read()

    try:
        df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,
            keep_default_na=False,
        )
    except Exception as exc:
        logger.error("Failed to parse CSV from s3://%s/%s: %s", s3_bucket, s3_key, exc)
        raise FileReadError(
            f"CSV parse failure for s3://{s3_bucket}/{s3_key}: {exc}"
        ) from exc

    # LOGIC — strip leading/trailing whitespace from all string-valued cells
    for col in df.columns:
        df[col] = df[col].str.strip()

    logger.info(
        "Successfully read %d rows from s3://%s/%s",
        len(df),
        s3_bucket,
        s3_key,
    )
    return df