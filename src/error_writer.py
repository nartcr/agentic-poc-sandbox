import io
import logging

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — mandatory column order per data contract
_ERROR_COLUMNS = [
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
    s3_client,
    bucket: str,
    error_prefix: str,
    desk_code: str,
    trade_date: str,
    rejected_df: pd.DataFrame,
) -> str:
    """
    Writes the rejected rows DataFrame as a CSV to S3 under the errors prefix.

    S3 key pattern: {error_prefix}{desk_code}_{trade_date}_errors.csv
    Returns the S3 key written, or "" if rejected_df is empty.
    """
    # LOGIC — skip write entirely when there are no rejected rows
    if rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return ""

    # LOGIC — build S3 key using the exact pattern from the data contract
    s3_key = f"{error_prefix}{desk_code}_{trade_date}_errors.csv"

    # LOGIC — reorder/select columns so rejection_reason is always last;
    #          only include columns that are actually present in the DataFrame
    #          to handle edge cases where optional columns may be absent.
    present_columns = [col for col in _ERROR_COLUMNS if col in rejected_df.columns]
    output_df = rejected_df[present_columns]

    # LOGIC — serialise to CSV in-memory; never touch /tmp/
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows).",
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