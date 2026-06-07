# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — expected CSV columns per data contract; used only for logging; enforcement is downstream
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    # BOILERPLATE — create S3 client using Lambda execution role (no hardcoded credentials)
    s3_client = boto3.client("s3")

    # LOGIC — download the S3 object bytes
    logger.info("Fetching s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    logger.info("Downloaded %d bytes from s3://%s/%s", len(raw_bytes), bucket, key)

    # LOGIC — decode as UTF-8 and parse CSV with all columns forced to str dtype
    # dtype=str ensures downstream validator sees raw string values and can detect malformed entries
    csv_text = raw_bytes.decode("utf-8")
    df = pd.read_csv(
        io.StringIO(csv_text),
        dtype=str,          # LOGIC — all columns as strings; type coercion deferred to validator
        keep_default_na=False,  # LOGIC — do not silently convert empty strings to NaN here
    )

    logger.info(
        "Parsed CSV: rows=%d columns=%s",
        len(df),
        list(df.columns),
    )

    # LOGIC — log a warning if any expected columns are missing, but do not raise here
    # (schema enforcement is the responsibility of row_validator)
    missing = [col for col in _EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        logger.warning(
            "CSV at s3://%s/%s is missing expected columns: %s", bucket, key, missing
        )

    return df