# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — expected columns per data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_csv(bucket: str, key: str) -> pd.DataFrame:
    # BOILERPLATE — create S3 client using Lambda execution role (no credentials in code)
    s3_client = boto3.client("s3")

    logger.info("Fetching s3://%s/%s", bucket, key)

    # LOGIC — fetch object from S3
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body_bytes = response["Body"].read()

    if not body_bytes.strip():
        raise ValueError(
            f"S3 object s3://{bucket}/{key} is empty (zero bytes after strip)"
        )

    # LOGIC — parse CSV; all columns read as str to preserve raw values for validation
    df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=str,
        keep_default_na=False,  # do not convert "NA", "NaN", etc. to NaN automatically
        encoding="utf-8",
    )

    logger.info(
        "Parsed CSV from s3://%s/%s: %d rows, columns=%s",
        bucket,
        key,
        len(df),
        list(df.columns),
    )

    # LOGIC — reject empty files (zero data rows after header parse)
    if len(df) == 0:
        raise ValueError(
            f"S3 object s3://{bucket}/{key} contains a header but zero data rows"
        )

    return df