# BOILERPLATE
import io
import logging

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — output column order is mandated by the data contract
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


def _build_s3_key(desk_code: str, trade_date: str) -> str:
    # LOGIC — key pattern: errors/{desk_code}_{trade_date}_positions_errors.csv
    return f"errors/{desk_code}_{trade_date}_positions_errors.csv"


def _serialize_to_csv(rejected_df: pd.DataFrame) -> bytes:
    # LOGIC — serialize in-memory to avoid /tmp/ usage; enforce column order
    output_df = rejected_df.reindex(columns=_ERROR_CSV_COLUMNS)
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False, encoding="utf-8")
    return buffer.getvalue().encode("utf-8")


def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date: str,
) -> str:
    """
    Write the rejected-rows DataFrame as a UTF-8 CSV to S3 under the errors/ prefix.
    Returns the full S3 key of the written file, or an empty string if rejected_df
    is empty (no file is written in that case).

    Satisfies: BAC-2 (error file written to S3 with rejection reasons, TAC-2).
    """
    # LOGIC — no-op when there are no rejected rows
    if rejected_df.empty:
        logger.info(
            "rejected_df is empty — no error file written for desk_code=%s trade_date=%s",
            desk_code,
            trade_date,
        )
        return ""

    s3_key = _build_s3_key(desk_code, trade_date)
    logger.info(
        "Writing error file: s3://%s/%s (%d rejected row(s))",
        bucket,
        s3_key,
        len(rejected_df),
    )

    # LOGIC — serialize rejected rows to CSV bytes in-memory
    csv_bytes = _serialize_to_csv(rejected_df)

    # BOILERPLATE — upload to S3 using boto3
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_bytes,
            ContentType="text/csv",
            ContentEncoding="utf-8",
        )
        logger.info(
            "Error file successfully written: s3://%s/%s (%d bytes)",
            bucket,
            s3_key,
            len(csv_bytes),
        )
    except Exception as exc:
        logger.error(
            "Failed to write error file to s3://%s/%s: %s",
            bucket,
            s3_key,
            exc,
            exc_info=True,
        )
        raise

    return s3_key