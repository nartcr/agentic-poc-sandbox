# BOILERPLATE
import io
import logging
from pathlib import Path

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — ordered columns for the error CSV per data contract
_ERROR_CSV_COLUMNS = [
    "_source_row",
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    source_key: str,
    errors_prefix: str,
) -> str:
    # LOGIC — derive deterministic error key from source key basename
    # e.g. positions/EQTY_2026-06-15_positions.csv
    #   -> errors/EQTY_2026-06-15_positions_errors.csv
    stem = Path(source_key).stem          # EQTY_2026-06-15_positions
    error_key = f"{errors_prefix}{stem}_errors.csv"

    # LOGIC — select and order columns; any column absent in rejected_df becomes NaN
    output_df = rejected_df.reindex(columns=_ERROR_CSV_COLUMNS)

    # LOGIC — serialize to UTF-8 CSV bytes in-memory (no filesystem writes)
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "write_error_file: wrote %d rejected rows to s3://%s/%s",
        len(output_df),
        bucket,
        error_key,
    )
    return error_key