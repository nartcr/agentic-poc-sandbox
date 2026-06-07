# BOILERPLATE
import io
import logging
from typing import Optional

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)


def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    bucket: str,
    error_prefix: str,
) -> Optional[str]:
    # LOGIC — if no rejections, write nothing and return None
    if rejected_df.empty:
        logger.info(
            "write_error_file: no rejected rows for desk_code=%s trade_date=%s; skipping upload",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — construct the S3 key from the error prefix and file naming convention
    s3_key = f"{error_prefix}{desk_code}_{trade_date}_rejected.csv"

    # LOGIC — serialize DataFrame to CSV in-memory (no /tmp/ paths)
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    logger.info(
        "write_error_file: uploading %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        s3_key,
    )

    # BOILERPLATE — S3 client created at call time (Lambda execution model, no persistent clients)
    s3_client = boto3.client("s3")

    # LOGIC — upload rejection CSV with explicit content type
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info("write_error_file: upload complete -> s3://%s/%s", bucket, s3_key)
    return s3_key