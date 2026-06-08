# BOILERPLATE
import io
import logging
import os

import boto3
import pandas as pd

from pipeline_exceptions import FileReadError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns expected in every incoming trade position CSV
_EXPECTED_COLUMNS = {
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
}


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    # LOGIC
    Stream a CSV object from S3 and return a raw DataFrame with all columns as str.

    Parameters
    ----------
    bucket : str
        S3 bucket name (value of os.environ["S3_BUCKET"] at call site).
    key : str
        Full S3 object key, e.g. 'incoming/EQDESK_2026-06-01_positions.csv'.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame with dtype=str for all columns.

    Raises
    ------
    FileReadError
        If the S3 object cannot be retrieved or the CSV cannot be parsed.
    """
    logger.info("Fetching S3 object: s3://%s/%s", bucket, key)

    # BOILERPLATE — build S3 client; credentials from Lambda execution role
    s3_client = boto3.client("s3")

    # LOGIC — retrieve object; wrap all S3 and CSV errors in FileReadError
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise FileReadError(
            f"Failed to retrieve S3 object s3://{bucket}/{key}: {exc}"
        ) from exc

    try:
        # LOGIC — read streaming body into memory buffer so pandas can seek
        body_bytes = response["Body"].read()
        buffer = io.BytesIO(body_bytes)

        # LOGIC — dtype=str prevents silent numeric/date coercion before validation
        df = pd.read_csv(buffer, dtype=str, keep_default_na=False)
    except Exception as exc:
        raise FileReadError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    logger.info(
        "CSV parsed: %d rows, %d columns — columns: %s",
        len(df),
        len(df.columns),
        list(df.columns),
    )

    # LOGIC — warn if any expected columns are missing; do not raise here
    # (row_validator will surface column-level issues per row)
    missing_cols = _EXPECTED_COLUMNS - set(df.columns)
    if missing_cols:
        logger.warning(
            "CSV from s3://%s/%s is missing expected columns: %s",
            bucket,
            key,
            sorted(missing_cols),
        )

    return df