# BOILERPLATE
import io
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def write_error_file(
    s3_client,
    bucket: str,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> str | None:
    # LOGIC
    if rejected_df.empty:
        logger.info("No rejected rows — skipping error file write.")
        return None

    # LOGIC: Format timestamp suffix as yyyymmddHHMMSS in ET (processing_ts is already ET-aware)
    ts_suffix = processing_ts.strftime("%Y%m%d%H%M%S")
    s3_key = f"errors/{desk_code}_{trade_date}_{ts_suffix}_errors.csv"

    # LOGIC: Serialise rejected DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # LOGIC: Upload error CSV to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows)",
        bucket,
        s3_key,
        len(rejected_df),
    )

    return s3_key