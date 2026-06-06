# BOILERPLATE
import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# REQUIRED CSV COLUMNS — defined here for reference; validation is in validator.py
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def download_file(s3_client, bucket: str, key: str) -> io.BytesIO:
    # LOGIC — downloads the S3 object into an in-memory BytesIO buffer
    logger.info("Downloading s3://%s/%s", bucket, key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            # LOGIC — surface a FileNotFoundError for missing keys
            raise FileNotFoundError(
                f"S3 object not found: s3://{bucket}/{key}"
            ) from exc
        # LOGIC — all other S3 errors become IOError
        raise IOError(
            f"Failed to retrieve s3://{bucket}/{key}: {exc}"
        ) from exc

    try:
        body_bytes = response["Body"].read()
    except Exception as exc:
        raise IOError(
            f"Failed to read body of s3://{bucket}/{key}: {exc}"
        ) from exc

    logger.info(
        "Downloaded %d bytes from s3://%s/%s", len(body_bytes), bucket, key
    )
    return io.BytesIO(body_bytes)


def parse_csv(file_bytes: io.BytesIO, source_key: str) -> pd.DataFrame:
    # LOGIC — parse raw CSV bytes into a DataFrame; all columns kept as strings
    file_bytes.seek(0)

    try:
        df = pd.read_csv(
            file_bytes,
            dtype=str,          # LOGIC — prevent silent type coercion before validation
            keep_default_na=False,  # LOGIC — preserve empty strings as "" not NaN
            na_values=[],       # LOGIC — disable pandas default NA string detection
        )
    except Exception as exc:
        raise ValueError(
            f"Could not parse CSV from {source_key}: {exc}"
        ) from exc

    # LOGIC — reject empty files (no header row or no data rows after header)
    if df.empty and len(df.columns) == 0:
        raise ValueError(
            f"CSV file {source_key} appears to be empty (no header row found)"
        )

    if df.empty:
        # File has a header row but zero data rows — still a parseable file;
        # downstream validation will handle the empty DataFrame gracefully.
        logger.warning(
            "CSV file %s parsed successfully but contains zero data rows",
            source_key,
        )

    # LOGIC — tag every row with the originating S3 key for traceability
    df["_source_file"] = source_key

    logger.info(
        "Parsed %d rows from %s (columns: %s)",
        len(df),
        source_key,
        list(df.columns),
    )
    return df