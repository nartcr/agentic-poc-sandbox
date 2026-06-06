# BOILERPLATE
import io
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

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

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# desk_code: one or more uppercase letters/digits; trade_date: exactly 8 digits (YYYYMMDD)
_FILENAME_PATTERN = re.compile(r"^([A-Z0-9]+)_(\d{8})_positions\.csv$")


def parse_filename(s3_key: str) -> tuple[str, str]:
    # LOGIC — extract basename from the full S3 key path
    basename = s3_key.split("/")[-1]
    match = _FILENAME_PATTERN.match(basename)
    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match required pattern "
            r"^[A-Z0-9]+_\d{8}_positions\.csv$"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    logger.info(
        "Parsed filename: desk_code=%s trade_date=%s from key=%s",
        desk_code,
        trade_date,
        s3_key,
    )
    return desk_code, trade_date


def read_position_file(
    s3_client, bucket: str, s3_key: str
) -> tuple[pd.DataFrame, str, str]:
    # LOGIC — parse desk_code and trade_date before attempting S3 read so
    # a bad filename fails fast before incurring an S3 network call
    desk_code, trade_date = parse_filename(s3_key)

    logger.info("Reading position file from s3://%s/%s", bucket, s3_key)

    # BOILERPLATE — retrieve object bytes from S3
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    body_bytes = response["Body"].read()

    # LOGIC — read all columns as str (dtype=object) to preserve raw values
    # for downstream validation; no type coercion at this stage
    raw_df = pd.read_csv(
        io.BytesIO(body_bytes),
        dtype=object,          # all columns as str / object — no coercion
        keep_default_na=False, # do not convert empty strings to NaN
        na_values=[],          # disable pandas default NA detection
    )

    logger.info(
        "Read %d rows and %d columns from s3://%s/%s",
        len(raw_df),
        len(raw_df.columns),
        bucket,
        s3_key,
    )

    # LOGIC — warn if expected columns are missing; do not raise here so
    # the validator can produce per-row rejection reasons rather than a
    # hard crash on schema mismatch
    actual_columns = list(raw_df.columns)
    missing_cols = [c for c in _EXPECTED_COLUMNS if c not in actual_columns]
    if missing_cols:
        logger.warning(
            "File s3://%s/%s is missing expected columns: %s",
            bucket,
            s3_key,
            missing_cols,
        )

    return raw_df, desk_code, trade_date