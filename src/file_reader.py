# BOILERPLATE
import io
import logging
from typing import Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# LOGIC
class FileReadError(Exception):
    """Raised when the S3 CSV file cannot be fetched or parsed."""


# LOGIC
def read_position_file(
    s3_client,
    bucket: str,
    s3_key: str,
) -> Tuple[pd.DataFrame, int]:
    """
    Download a CSV file from S3 and return a raw string-typed DataFrame
    plus the row count (excluding the header).

    Parameters
    ----------
    s3_client : boto3 S3 client
    bucket    : S3 bucket name
    s3_key    : full S3 object key, e.g. incoming/DESK1_2024-01-15_positions.csv

    Returns
    -------
    (df, row_count) where df has all columns read as str.

    Raises
    ------
    FileReadError if the object does not exist or the body cannot be parsed.
    """
    # LOGIC — fetch object from S3
    logger.info("Fetching S3 object: s3://%s/%s", bucket, s3_key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "S3 ClientError reading s3://%s/%s — code=%s",
            bucket,
            s3_key,
            error_code,
        )
        raise FileReadError(
            f"Cannot fetch S3 object s3://{bucket}/{s3_key}: {error_code}"
        ) from exc

    # LOGIC — read body bytes and parse as UTF-8 CSV
    try:
        body_bytes = response["Body"].read()
        csv_buffer = io.StringIO(body_bytes.decode("utf-8"))

        # LOGIC — all columns read as str; no type inference from pandas
        df = pd.read_csv(
            csv_buffer,
            delimiter=",",
            dtype=str,
            keep_default_na=False,   # do not convert empty strings to NaN
            encoding=None,           # already decoded above
        )
    except UnicodeDecodeError as exc:
        logger.error("UTF-8 decode failure for s3://%s/%s", bucket, s3_key)
        raise FileReadError(
            f"UTF-8 decode failure for s3://{bucket}/{s3_key}"
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Failed to parse CSV from s3://%s/%s: %s", bucket, s3_key, exc
        )
        raise FileReadError(
            f"Failed to parse CSV from s3://{bucket}/{s3_key}: {exc}"
        ) from exc

    # LOGIC — strip leading/trailing whitespace from all string column values
    for col in df.columns:
        df[col] = df[col].str.strip()

    row_count = len(df)
    logger.info(
        "Successfully parsed s3://%s/%s — %d rows (excluding header)",
        bucket,
        s3_key,
        row_count,
    )
    return df, row_count