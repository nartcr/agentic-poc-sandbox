# BOILERPLATE
import io
import logging

logger = logging.getLogger(__name__)


def write_error_file(
    s3_client,
    rejected_df,
    bucket: str,
    error_prefix: str,
    desk_code: str,
    trade_date: str,
) -> str:
    # LOGIC — construct the S3 key following the naming convention from the data contract
    filename = f"{desk_code}_{trade_date}_positions_errors.csv"
    s3_key = f"{error_prefix}{filename}"

    # LOGIC — serialize to CSV in-memory; always write, even when rejected_df is empty,
    # so downstream systems receive a consistent artifact (header-only file on zero rejections)
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows, %d bytes)",
        bucket,
        s3_key,
        len(rejected_df),
        len(csv_bytes),
    )

    # LOGIC — upload CSV bytes to S3 using the pre-configured client
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info("Error file written successfully: s3://%s/%s", bucket, s3_key)

    return s3_key