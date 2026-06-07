# BOILERPLATE
import io
import logging
import boto3

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — column order mandated by the data contract
_ERROR_FILE_COLUMNS = [
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
    desk_code: str,
    trade_date: str,
) -> str:
    # LOGIC — if there are no rejected rows, do not create an empty error file
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return ""

    # LOGIC — construct the exact S3 key from the data contract path pattern
    error_key = f"errors/{desk_code}_{trade_date}_positions_errors.csv"

    # LOGIC — reorder/select columns to match the data contract exactly;
    #          any extra columns (e.g. internal processing flags) are dropped
    output_df = rejected_df.reindex(columns=_ERROR_FILE_COLUMNS)

    # LOGIC — serialise to UTF-8 CSV bytes entirely in memory (no /tmp/ path)
    string_buffer = io.StringIO()
    output_df.to_csv(string_buffer, index=False)
    csv_bytes = string_buffer.getvalue().encode("utf-8")
    bytes_buffer = io.BytesIO(csv_bytes)

    # BOILERPLATE — upload to S3 using the existing bucket (never create buckets)
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=bytes_buffer.getvalue(),
        ContentType="text/csv; charset=utf-8",
    )

    logger.info(
        "Wrote %d rejected row(s) to s3://%s/%s",
        len(output_df),
        bucket,
        error_key,
    )

    return error_key