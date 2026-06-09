# BOILERPLATE
import io
import logging

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — expected columns as defined in the data contract
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


class FileIngestionError(Exception):
    """
    # LOGIC
    Raised when the S3 file cannot be retrieved, is empty, or cannot be parsed
    into a DataFrame with the expected structure.
    """


def read_position_file(bucket: str, key: str, s3_client) -> tuple:
    """
    # LOGIC
    Download the CSV file from S3 and parse it into a pandas DataFrame.

    All columns are read as strings (dtype=str) to prevent silent type coercion
    before the validation step.

    Parameters
    ----------
    bucket : str
        S3 bucket name.
    key : str
        S3 object key (e.g. 'incoming/DESK_2026-01-15_positions.csv').
    s3_client : boto3 S3 client
        Pre-initialised boto3 S3 client.

    Returns
    -------
    tuple[pd.DataFrame, int]
        (raw_df, total_row_count) where raw_df has all columns as str dtype
        and total_row_count is the number of data rows (excluding header).

    Raises
    ------
    FileIngestionError
        If the S3 get_object call fails, the file is empty, or the file cannot
        be parsed as CSV.
    """
    # LOGIC — attempt to retrieve the object from S3
    try:
        logger.info("Fetching s3://%s/%s", bucket, key)
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        raise FileIngestionError(
            f"Failed to retrieve s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — read the response body bytes
    try:
        body_bytes = response["Body"].read()
    except Exception as exc:
        raise FileIngestionError(
            f"Failed to read response body for s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — guard against empty files before attempting CSV parse
    if not body_bytes or body_bytes.strip() == b"":
        raise FileIngestionError(
            f"File s3://{bucket}/{key} is empty."
        )

    # LOGIC — decode and parse CSV, all columns as str to prevent type coercion
    try:
        body_str = body_bytes.decode("utf-8")
        raw_df = pd.read_csv(
            io.StringIO(body_str),
            dtype=str,
            keep_default_na=False,  # preserve empty strings as "" not NaN
        )
    except Exception as exc:
        raise FileIngestionError(
            f"Failed to parse CSV from s3://{bucket}/{key}: {exc}"
        ) from exc

    # LOGIC — guard against header-only or truly empty CSV
    if raw_df.empty:
        raise FileIngestionError(
            f"File s3://{bucket}/{key} contains no data rows (header only or empty)."
        )

    # LOGIC — log column names to aid debugging if schema drifts
    logger.info(
        "Parsed CSV columns from s3://%s/%s: %s",
        bucket,
        key,
        list(raw_df.columns),
    )

    # LOGIC — validate that all expected columns are present
    missing_columns = [col for col in _EXPECTED_COLUMNS if col not in raw_df.columns]
    if missing_columns:
        raise FileIngestionError(
            f"File s3://{bucket}/{key} is missing expected columns: {missing_columns}. "
            f"Found columns: {list(raw_df.columns)}"
        )

    total_row_count = len(raw_df)
    logger.info(
        "Successfully ingested %d data rows from s3://%s/%s",
        total_row_count,
        bucket,
        key,
    )

    return raw_df, total_row_count