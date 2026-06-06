# BOILERPLATE
import io
import logging

import pandas as pd

from src import s3_client

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — mandated column order from data contract
_ERROR_CSV_COLUMNS = [
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
    desk_code: str,
    trade_date: str,
    bucket: str,
) -> str:
    # LOGIC — build the S3 key following the errors/ prefix pattern
    s3_key = f"errors/{desk_code}_{trade_date}_errors.csv"

    logger.info(
        "Writing error file: bucket=%s key=%s rows=%d",
        bucket,
        s3_key,
        len(rejected_df),
    )

    # LOGIC — enforce column order; add any missing columns as empty strings
    # so the CSV is always well-formed even if rejected_df is a partial frame
    output_df = rejected_df.reindex(columns=_ERROR_CSV_COLUMNS)

    # LOGIC — serialise to UTF-8 CSV bytes without the pandas index column
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    # LOGIC — upload via the approved s3_client wrapper
    s3_client.upload_bytes(
        bucket=bucket,
        key=s3_key,
        data=csv_bytes,
        content_type="text/csv",
    )

    logger.info("Error file uploaded successfully: %s", s3_key)
    return s3_key