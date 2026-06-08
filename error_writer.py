# BOILERPLATE
import io
import logging
import os
import re

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _derive_error_key(source_key: str) -> str:
    # LOGIC — replace incoming/ prefix with errors/ and inject _errors before .csv
    # Example: incoming/EQTY_2026-06-01_positions.csv
    #      ->  errors/EQTY_2026-06-01_positions_errors.csv
    #
    # Strategy: strip the leading prefix up to and including the last '/' to get
    # the bare filename, transform the filename, then prepend errors/.
    # Using re.sub so the rule is explicit and cannot be broken by nested slashes.

    # LOGIC — replace leading path segments (e.g. "incoming/") with "errors/"
    error_key = re.sub(r"^[^/]+/", "errors/", source_key)

    # LOGIC — inject "_errors" before the ".csv" extension (case-insensitive)
    error_key = re.sub(r"\.csv$", "_errors.csv", error_key, flags=re.IGNORECASE)

    return error_key


def write_error_file(bucket: str, source_key: str, rejected_df: pd.DataFrame) -> str:
    """Serialize rejected rows (with rejection_reason) to CSV and write to S3.

    Returns the S3 key of the written error file.
    """
    # LOGIC — derive destination key from source key
    error_s3_key = _derive_error_key(source_key)

    # LOGIC — serialize rejected_df to CSV bytes (all original columns + rejection_reason)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3
    s3_client = boto3.client("s3")

    logger.info(
        "Writing %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        error_s3_key,
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=error_s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info("Error file written: s3://%s/%s", bucket, error_s3_key)

    return error_s3_key