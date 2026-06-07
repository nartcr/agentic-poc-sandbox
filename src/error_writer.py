# BOILERPLATE
import io
import logging

logger = logging.getLogger(__name__)


def write_error_file(
    s3_client,
    bucket: str,
    desk_code: str,
    trade_date: str,
    rejected_df,
) -> "str | None":
    """
    Serialise rejected_df to a UTF-8 CSV and upload it to S3 under the
    errors/ prefix.

    Returns the S3 key string if a file was written, None if rejected_df
    is empty.
    """
    # LOGIC — skip write entirely if there are no rejected rows
    if rejected_df is None or len(rejected_df) == 0:
        logger.info(
            "write_error_file: no rejected rows for desk_code=%s trade_date=%s; "
            "skipping error file upload",
            desk_code,
            trade_date,
        )
        return None

    # LOGIC — construct S3 key using the exact pattern from the data contract
    s3_key = f"errors/{desk_code}_{trade_date}_errors.csv"

    # LOGIC — serialise DataFrame to UTF-8 CSV bytes in memory (no /tmp/ paths)
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    # LOGIC — upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "write_error_file: uploaded %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        s3_key,
    )

    return s3_key