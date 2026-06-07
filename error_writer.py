# BOILERPLATE
import io
import logging
import os
import re

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def _build_error_s3_key(desk_code: str, trade_date: str, processing_timestamp_et: str) -> str:
    # LOGIC — strip all non-alphanumeric characters except 'T' positional separator
    # Input format: ISO 8601 e.g. "2024-06-15T14:35:22-04:00"
    # Target token: "YYYYMMDDTHHMMSS"
    timestamp_token = re.sub(r"[^0-9T]", "", processing_timestamp_et)
    # re.sub strips colons and dashes; the literal 'T' is preserved by [^0-9T]
    # Result from "2024-06-15T14:35:22-04:00" => "20240615T143522040000"
    # Truncate to 15 chars: YYYYMMDDTHHMMSS
    timestamp_token = timestamp_token[:15]
    return f"errors/{desk_code}_{trade_date}_errors_{timestamp_token}.csv"


def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    processing_timestamp_et: str,
) -> str:
    """
    Writes rejected rows (with rejection_reason column) to S3 as a CSV.
    Writes a header-only CSV when rejected_df is empty.
    Returns the S3 key written.
    """
    # BOILERPLATE
    bucket = os.environ["S3_BUCKET"]
    s3_client = boto3.client("s3")

    # LOGIC — derive S3 key from design-mandated pattern
    s3_key = _build_error_s3_key(desk_code, trade_date, processing_timestamp_et)

    # LOGIC — serialise DataFrame (or empty frame with correct columns) to CSV in-memory
    if rejected_df.empty:
        # Ensure the mandatory columns plus rejection_reason are present in the header
        mandatory_columns = [
            "trade_id",
            "desk_code",
            "trade_date",
            "instrument_type",
            "notional_amount",
            "currency",
            "counterparty_id",
            "rejection_reason",
        ]
        # Use whatever columns are present if the frame was constructed with them;
        # fall back to the canonical list when the frame has no columns at all.
        if rejected_df.columns.tolist():
            output_df = rejected_df
        else:
            output_df = pd.DataFrame(columns=mandatory_columns)
    else:
        output_df = rejected_df

    # LOGIC — write CSV to in-memory buffer (no filesystem writes)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows)",
        bucket,
        s3_key,
        len(output_df),
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info("Error file written successfully: s3://%s/%s", bucket, s3_key)
    return s3_key