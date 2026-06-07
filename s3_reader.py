# BOILERPLATE
import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# BOILERPLATE — module-level client; re-used across warm Lambda invocations
_s3_client = None


def _get_s3_client():
    # BOILERPLATE — lazy singleton, avoids re-creating client on every call
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def read_position_file(bucket: str, key: str) -> tuple:
    """
    Download a single CSV position file from S3 and return it as a DataFrame.

    All columns are read as strings (dtype=str) so that downstream validation
    controls type checking rather than pandas inference.

    Returns:
        (raw_df, s3_key) where raw_df is a pd.DataFrame and s3_key is the key
        passed in (carried through for traceability).

    Raises:
        FileNotFoundError: if the S3 key does not exist.
        ValueError: if the file contains zero data rows after the header.
    """
    # LOGIC — attempt to fetch the object; surface a clean error on missing key
    client = _get_s3_client()
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            raise FileNotFoundError(
                f"S3 object not found: s3://{bucket}/{key}"
            ) from exc
        # LOGIC — re-raise unexpected AWS errors unchanged
        raise

    # LOGIC — read the entire body into memory before passing to pandas
    # to avoid issues with streaming reads being exhausted mid-parse
    body_bytes = response["Body"].read()
    raw_df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,          # LOGIC — preserve all values as strings for validation
        keep_default_na=False,  # LOGIC — treat empty cells as "" not NaN so mandatory-field check works correctly
    )

    # LOGIC — reject completely empty files before returning
    if len(raw_df) == 0:
        raise ValueError(
            f"Position file contains zero data rows (header only): s3://{bucket}/{key}"
        )

    # BOILERPLATE — structured log at INFO so operators can trace file ingestion
    logger.info(
        "Read position file: key=%s rows=%d columns=%s",
        key,
        len(raw_df),
        list(raw_df.columns),
    )

    return raw_df, key