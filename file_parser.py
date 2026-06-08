# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_s3_csv(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads a CSV file from S3 and returns a raw pandas DataFrame.

    All columns are read as strings (dtype=str) with keep_default_na=False so
    that raw values are preserved exactly as-is for downstream validation.
    No type coercion is performed here.

    Raises ValueError if the file is empty or cannot be parsed.
    """
    # BOILERPLATE — S3 client uses Lambda execution role; no explicit credentials
    s3_client = boto3.client("s3")

    logger.info("Fetching S3 object: s3://%s/%s", bucket, key)

    # LOGIC — retrieve raw bytes from S3
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body_bytes = response["Body"].read()
    except Exception as exc:
        logger.error(
            "Failed to read s3://%s/%s: %s", bucket, key, exc
        )
        raise

    # LOGIC — guard against an empty file body before attempting CSV parse
    if not body_bytes or body_bytes.strip() == b"":
        raise ValueError(
            f"S3 object s3://{bucket}/{key} is empty — no data to process."
        )

    # LOGIC — parse CSV preserving all values as raw strings for validation
    try:
        df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,
            keep_default_na=False,
        )
    except Exception as exc:
        logger.error(
            "Failed to parse CSV from s3://%s/%s: %s", bucket, key, exc
        )
        raise ValueError(
            f"CSV parsing failed for s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — a CSV with only a header row (zero data rows) is still a valid
    # empty DataFrame and should be returned as-is; the caller handles empty DFs.
    # A file whose body has content but yields no columns is an unreadable file.
    if df.columns.empty:
        raise ValueError(
            f"S3 object s3://{bucket}/{key} produced a DataFrame with no columns. "
            "File may be malformed."
        )

    logger.info(
        "Successfully parsed CSV: s3://%s/%s — %d rows, %d columns",
        bucket,
        key,
        len(df),
        len(df.columns),
    )

    return df