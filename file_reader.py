# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — the seven columns the pipeline expects; validated downstream by row_validator
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_position_file(bucket: str, key: str) -> tuple:
    """
    Read a CSV file from S3 and return a raw DataFrame plus the total row count.

    All columns are read as strings (dtype=str) to prevent pandas type coercion
    before validation occurs in row_validator.

    Returns:
        (df, total_row_count) where total_row_count == len(df).
    """
    # BOILERPLATE — create S3 client; credentials come from Lambda execution role
    s3_client = boto3.client("s3")

    logger.info("Reading S3 object: s3://%s/%s", bucket, key)

    # LOGIC — stream the S3 object body into memory
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()
    logger.info("S3 object read: %d bytes", len(body_bytes))

    # LOGIC — parse CSV with dtype=str to prevent any automatic type casting;
    # keep_default_na=False so empty strings remain empty strings (not NaN)
    # before the validator applies its own null/empty checks.
    df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,
    )

    total_row_count = len(df)
    logger.info(
        "CSV parsed: %d rows, columns=%s", total_row_count, list(df.columns)
    )

    # LOGIC — warn if any expected columns are absent; row_validator will reject
    # affected rows, but logging here aids debugging.
    missing_cols = [c for c in _EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        logger.warning(
            "Input CSV is missing expected columns: %s — affected rows will be rejected",
            missing_cols,
        )

    return df, total_row_count