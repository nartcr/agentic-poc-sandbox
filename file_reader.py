# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

from pipeline_exceptions import FileReadError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    # LOGIC
    Retrieve a CSV object from S3 and parse it into a pandas DataFrame.

    All columns are read as strings (dtype=str) to preserve original values
    for downstream validation — no implicit type coercion occurs here.

    Parameters
    ----------
    bucket : str
        Name of the S3 bucket (value of os.environ["S3_BUCKET"]).
    key : str
        S3 object key, expected to match
        ``incoming/{desk_code}_{trade_date}_positions.csv``.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame with all columns typed as object/str.

    Raises
    ------
    FileReadError
        If the S3 object cannot be retrieved or the body cannot be parsed
        as a valid comma-delimited CSV.
    """
    # BOILERPLATE — create S3 client (no credentials in code; boto3 uses IAM role)
    s3_client = boto3.client("s3")

    # LOGIC — retrieve the S3 object
    logger.info("Fetching s3://%s/%s", bucket, key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        logger.error(
            "Failed to retrieve s3://%s/%s: %s", bucket, key, exc
        )
        raise FileReadError(
            f"Cannot retrieve S3 object s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — read the response body into memory and parse as CSV
    try:
        body_bytes: bytes = response["Body"].read()
        logger.info(
            "Retrieved %d bytes from s3://%s/%s", len(body_bytes), bucket, key
        )
        df = pd.read_csv(
            io.BytesIO(body_bytes),
            dtype=str,          # preserve original string values for validation
            keep_default_na=False,  # do not convert empty strings to NaN automatically
        )
    except Exception as exc:
        logger.error(
            "Failed to parse CSV from s3://%s/%s: %s", bucket, key, exc
        )
        raise FileReadError(
            f"Cannot parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — strip leading/trailing whitespace from column names
    df.columns = [col.strip() for col in df.columns]

    logger.info(
        "Parsed CSV — rows=%d columns=%s", len(df), list(df.columns)
    )
    return df