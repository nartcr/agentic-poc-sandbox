# BOILERPLATE
import io
import logging
import os

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
    """Write rejected rows with rejection reasons to S3 error CSV.

    Returns the S3 key written, or None if there were no rejected rows.
    """
    # LOGIC — guard: nothing to write
    if rejected_df is None or rejected_df.empty:
        logger.debug(
            "No rejected rows to write for desk_code=%s trade_date=%s",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — build the canonical S3 key from the DATA CONTRACTS
    # Pattern: errors/{desk_code}_{trade_date}_errors.csv
    s3_key = f"errors/{desk_code}_{trade_date}_errors.csv"

    # LOGIC — serialize DataFrame to CSV in-memory (no /tmp/ paths)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — S3 client and upload
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv; charset=utf-8",
    )

    logger.info(
        "Wrote %d rejected row(s) to s3://%s/%s",
        len(rejected_df),
        bucket,
        s3_key,
    )
    return s3_key