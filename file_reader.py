# BOILERPLATE
import io
import logging
import os

import boto3
import botocore.exceptions
import pandas as pd

from exceptions import FileReadError

logger = logging.getLogger(__name__)

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# trade_date is YYYY-MM-DD (contains hyphens), so we split on '_positions.csv' first
_FILENAME_SUFFIX = "_positions.csv"


def _parse_filename_metadata(key: str) -> tuple[str, str]:
    """
    Extract desk_code and trade_date from a key matching
    {prefix}{desk_code}_{trade_date}_positions.csv.

    Returns (desk_code, trade_date).
    Raises FileReadError if the filename does not match the expected pattern.
    """
    # LOGIC
    basename = os.path.basename(key)
    if not basename.endswith(_FILENAME_SUFFIX):
        raise FileReadError(
            key,
            f"Filename '{basename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'.",
        )
    stem = basename[: -len(_FILENAME_SUFFIX)]  # e.g. "EQTY_2024-01-15"
    # Split on first underscore only — desk_code must not contain underscores
    parts = stem.split("_", 1)
    if len(parts) != 2:
        raise FileReadError(
            key,
            f"Filename stem '{stem}' cannot be split into desk_code and trade_date.",
        )
    desk_code, trade_date = parts
    return desk_code, trade_date


def read_position_file(bucket: str, key: str) -> tuple[pd.DataFrame, str]:
    """
    Download a CSV position file from S3 and parse it into a raw DataFrame.

    All columns are read as str (dtype=str) to defer type validation to validator.py.

    Returns (dataframe, source_filename) where source_filename is the S3 key basename.
    Also attaches .desk_code and .trade_date as DataFrame attributes for cross-validation.

    Raises FileReadError on S3 retrieval failure or CSV parse failure.
    """
    # BOILERPLATE
    source_filename = os.path.basename(key)
    logger.info("Reading position file from s3://%s/%s", bucket, key)

    # LOGIC — parse filename metadata before S3 call to fail fast on bad key
    desk_code, trade_date = _parse_filename_metadata(key)

    # LOGIC — download from S3
    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body_bytes = response["Body"].read()
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise FileReadError(key, f"S3 ClientError [{error_code}] retrieving object.") from exc
    except Exception as exc:
        raise FileReadError(
            key, f"Unexpected error retrieving S3 object: {type(exc).__name__}"
        ) from exc

    # LOGIC — parse CSV; all columns as string
    if not body_bytes.strip():
        raise FileReadError(key, "File is empty.")

    try:
        df = pd.read_csv(io.BytesIO(body_bytes), dtype=str)
    except Exception as exc:
        raise FileReadError(key, f"CSV parse error: {exc}") from exc

    if df.empty:
        raise FileReadError(key, "CSV file contains no data rows.")

    # LOGIC — attach filename metadata as DataFrame attributes
    df.attrs["desk_code"] = desk_code
    df.attrs["trade_date"] = trade_date

    logger.info(
        "Read %d rows from '%s' (desk_code=%s, trade_date=%s)",
        len(df),
        source_filename,
        desk_code,
        trade_date,
    )
    return df, source_filename