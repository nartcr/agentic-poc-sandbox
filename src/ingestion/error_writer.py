# BOILERPLATE
import io
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def write_error_file(s3_bucket: str, source_s3_key: str, rejected_df) -> str:
    # LOGIC — derive error S3 key from source key
    # Source pattern:  inbound/{desk_code}_{trade_date}_positions.csv
    # Target pattern:  errors/{desk_code}_{trade_date}_positions_errors.csv
    error_key = _derive_error_key(source_s3_key)

    # LOGIC — serialize rejected DataFrame to CSV bytes (UTF-8, with header)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3 using IAM role credentials
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Wrote %d rejected row(s) to s3://%s/%s",
        len(rejected_df),
        s3_bucket,
        error_key,
    )
    return error_key


def _derive_error_key(source_s3_key: str) -> str:
    # LOGIC — replace the inbound/ prefix with errors/ and insert _errors before .csv
    filename = os.path.basename(source_s3_key)
    if filename.endswith(".csv"):
        base = filename[:-4]          # strip .csv
        error_filename = base + "_errors.csv"
    else:
        error_filename = filename + "_errors.csv"
    return f"errors/{error_filename}"