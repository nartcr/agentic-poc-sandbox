import io
import logging
import os

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — ordered columns for the error CSV output per data contract
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
    s3_client,
) -> "str | None":
    # LOGIC — do nothing if there are no rejected rows (idempotent: no file written)
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    s3_bucket = os.environ["S3_BUCKET"]
    s3_error_prefix = os.environ["S3_ERROR_PREFIX"]
    error_key = f"{s3_error_prefix}{desk_code}_{trade_date}_errors.csv"

    # LOGIC — ensure all expected columns are present; fill missing with empty string
    output_df = rejected_df.copy()
    for col in _ERROR_CSV_COLUMNS:
        if col not in output_df.columns:
            output_df[col] = ""

    # LOGIC — write only the defined columns in the specified order
    output_df = output_df[_ERROR_CSV_COLUMNS]

    # LOGIC — serialize to CSV in memory (no /tmp/ disk writes)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows)",
        s3_bucket,
        error_key,
        len(output_df),
    )

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written successfully: key=%s rows=%d",
        error_key,
        len(output_df),
    )

    return error_key