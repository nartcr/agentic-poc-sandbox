# BOILERPLATE
import io
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC
def write_error_file(
    s3_client,
    rejected_df: pd.DataFrame,
    bucket: str,
    error_prefix: str,
    desk_code: str,
    trade_date: str,
    processed_at: datetime,
) -> "str | None":
    """Write rejected rows to S3 as a CSV error file.

    Returns the S3 key of the written file, or None if there are no rejections.
    """
    # LOGIC — skip write entirely when no rejections exist
    if rejected_df is None or len(rejected_df) == 0:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s — error file not written.",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — build the output S3 key using ET-localised processed_at timestamp
    timestamp_suffix = processed_at.strftime("%Y%m%d%H%M%S")
    s3_key = f"{error_prefix}{desk_code}_{trade_date}_errors_{timestamp_suffix}.csv"

    # LOGIC — select and order the output columns exactly as specified in the data contract
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

    # LOGIC — retain only the columns that are present; rejection_reason must always be present
    available_columns = [col for col in output_columns if col in rejected_df.columns]
    output_df = rejected_df[available_columns]

    # LOGIC — serialise the DataFrame to CSV in memory (no /tmp/ paths)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written: s3://%s/%s (%d rejected rows).",
        bucket,
        s3_key,
        len(output_df),
    )

    return s3_key