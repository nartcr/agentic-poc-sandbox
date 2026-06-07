# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — expected mandatory columns per data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_csv_from_s3(bucket: str, key: str) -> tuple[pd.DataFrame, int]:
    """
    Reads a CSV position file from S3 and returns a raw DataFrame (all columns as str)
    and the total row count.

    All columns are read as strings (dtype=str) to prevent silent type coercion
    before the validation step.

    Raises a descriptive exception if the S3 object cannot be retrieved or the
    CSV cannot be parsed.
    """
    # BOILERPLATE — create S3 client (credentials resolved by Lambda execution role at runtime)
    s3_client = boto3.client("s3")

    # LOGIC — retrieve object from S3
    logger.info("Fetching s3://%s/%s", bucket, key)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            "S3 object not found: s3://{}/{}".format(bucket, key)
        )
    except Exception as exc:
        raise RuntimeError(
            "Failed to retrieve S3 object s3://{}/{}: {}".format(bucket, key, exc)
        ) from exc

    # LOGIC — read raw bytes from streaming body
    try:
        raw_bytes = response["Body"].read()
    except Exception as exc:
        raise RuntimeError(
            "Failed to read body of S3 object s3://{}/{}: {}".format(bucket, key, exc)
        ) from exc

    # LOGIC — parse CSV; all columns forced to str to prevent silent coercion before validation
    try:
        raw_df = pd.read_csv(
            io.BytesIO(raw_bytes),
            dtype=str,           # preserve all values as strings
            keep_default_na=False,  # do not convert empty strings to NaN silently
            na_values=[""],      # only treat empty string as NaN so we can detect nulls
        )
    except Exception as exc:
        raise ValueError(
            "Failed to parse CSV from s3://{}/{}: {}".format(bucket, key, exc)
        ) from exc

    # LOGIC — validate that all mandatory columns are present in the file
    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in raw_df.columns]
    if missing_columns:
        raise ValueError(
            "CSV at s3://{}/{} is missing mandatory columns: {}".format(
                bucket, key, missing_columns
            )
        )

    # LOGIC — row count is the number of data rows (excluding header)
    total_rows = len(raw_df)
    logger.info(
        "Parsed CSV from s3://%s/%s: %d rows, columns=%s",
        bucket,
        key,
        total_rows,
        list(raw_df.columns),
    )

    return raw_df, total_rows