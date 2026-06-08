# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_s3_csv(bucket_name: str, object_key: str) -> pd.DataFrame:
    """Download a CSV from S3 and return a raw DataFrame with all columns as strings.

    No type coercion is performed here. Column headers and string cell values
    are stripped of leading/trailing whitespace. An empty file returns an
    empty DataFrame — the caller (row_validator) handles that case.
    """
    # BOILERPLATE — boto3 client; credentials from IAM execution role
    s3_client = boto3.client("s3")

    logger.info("Downloading s3://%s/%s", bucket_name, object_key)

    # LOGIC — stream the S3 object body into an in-memory buffer (no /tmp/ writes)
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    body_bytes: bytes = response["Body"].read()

    logger.info(
        "Downloaded %d bytes from s3://%s/%s",
        len(body_bytes),
        bucket_name,
        object_key,
    )

    # LOGIC — parse CSV with all columns forced to str; suppress pandas NA inference
    df: pd.DataFrame = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,
    )

    # LOGIC — strip whitespace from column headers
    df.columns = [col.strip() for col in df.columns]

    # LOGIC — strip whitespace from all string cell values without altering dtypes
    for col in df.columns:
        df[col] = df[col].str.strip()

    logger.info(
        "Parsed CSV: %d rows, %d columns: %s",
        len(df),
        len(df.columns),
        list(df.columns),
    )

    return df