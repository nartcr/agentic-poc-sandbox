# BOILERPLATE
import io
import logging
import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC
def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date: str,
) -> str | None:
    """
    Serialise rejected_df to CSV and write it to S3 under the errors/ prefix.
    Returns the S3 key of the written file, or None if rejected_df is empty.
    """
    # LOGIC — early exit when there are no rejected rows
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    s3_key = f"errors/{desk_code}_{trade_date}_positions_errors.csv"

    # LOGIC — define column ordering per data contract
    output_columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
        "rejection_reason",
    ]

    # LOGIC — only keep columns that are present in the DataFrame
    # (guards against missing metadata columns while preserving contract columns)
    columns_to_write = [c for c in output_columns if c in rejected_df.columns]

    # LOGIC — serialise to CSV in-memory
    buffer = io.StringIO()
    rejected_df[columns_to_write].to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows).",
        bucket,
        s3_key,
        len(rejected_df),
    )

    return s3_key