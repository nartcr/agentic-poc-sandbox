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
    # LOGIC — read a raw CSV from S3 into a fully string-typed DataFrame
    # No type coercion: dtype=str, keep_default_na=False ensures the validator
    # receives raw string values and applies business rules itself.

    logger.info("Reading s3://%s/%s", bucket, key)

    # BOILERPLATE — boto3 S3 client; credentials come from Lambda execution role
    s3_client = boto3.client("s3")

    # LOGIC — fetch object bytes from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    # LOGIC — decode as UTF-8 before pandas parse
    try:
        body_str = body_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"File s3://{bucket}/{key} could not be decoded as UTF-8: {exc}"
        ) from exc

    # LOGIC — guard against completely empty files before attempting parse
    if not body_str.strip():
        raise ValueError(f"File s3://{bucket}/{key} is empty")

    # LOGIC — parse CSV with all columns as raw strings; suppress pandas NA inference
    try:
        df = pd.read_csv(
            io.StringIO(body_str),
            dtype=str,
            keep_default_na=False,
        )
    except Exception as exc:
        raise ValueError(
            f"File s3://{bucket}/{key} could not be parsed as CSV: {exc}"
        ) from exc

    # LOGIC — treat a parsed-but-empty DataFrame (header only, zero data rows) as an error
    if df.empty:
        raise ValueError(
            f"File s3://{bucket}/{key} contains a header row but no data rows"
        )

    logger.info(
        "Parsed %d rows and %d columns from s3://%s/%s",
        len(df),
        len(df.columns),
        bucket,
        key,
    )

    return df