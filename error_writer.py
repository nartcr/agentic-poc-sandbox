# BOILERPLATE
import io
import json
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


class ErrorWriterError(Exception):
    """Raised when writing the error file or manifest to S3 fails."""


# LOGIC
def write_error_file(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date_str: str,
    s3_client,
    now_et: datetime,
) -> str:
    """
    Serialize rejected_df to CSV and upload to S3 under the errors/ prefix.

    S3 key pattern:
        errors/{desk_code}_{trade_date}_positions_errors_{YYYYMMDD_HHMMSS}.csv

    Returns the S3 key of the written error file.
    Raises ErrorWriterError on any S3 write failure.
    """
    # LOGIC — build the timestamped S3 key
    timestamp_str = now_et.strftime("%Y%m%d_%H%M%S")
    error_key = (
        f"errors/{desk_code}_{trade_date_str}_positions_errors_{timestamp_str}.csv"
    )

    # LOGIC — serialize DataFrame to CSV in memory; never touch the filesystem
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    row_count = len(rejected_df)
    logger.info(
        "Writing error file to s3://%s/%s (%d rejected rows)",
        bucket,
        error_key,
        row_count,
    )

    # LOGIC — upload to S3
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=error_key,
            Body=csv_bytes,
            ContentType="text/csv; charset=utf-8",
        )
    except Exception as exc:
        logger.error(
            "Failed to write error file to s3://%s/%s: %s",
            bucket,
            error_key,
            exc,
        )
        raise ErrorWriterError(
            f"Failed to write error file to s3://{bucket}/{error_key}: {exc}"
        ) from exc

    logger.info("Error file written successfully: s3://%s/%s", bucket, error_key)
    return error_key


# LOGIC
def write_error_manifest(
    bucket: str,
    desk_code: str,
    trade_date_str: str,
    error_key: str,
    row_count: int,
    s3_client,
    now_et: datetime,
) -> None:
    """
    Write (or overwrite) a manifest JSON at a predictable key so downstream
    consumers can locate the latest error file without guessing its timestamp.

    Manifest S3 key pattern (no timestamp — always overwritten):
        manifests/{desk_code}_{trade_date}_errors_manifest.json

    Manifest JSON structure:
        {
            "error_file_key": "<actual S3 key with timestamp>",
            "generated_at_et": "<ISO 8601 with ET offset>",
            "row_count": <int>
        }

    Raises ErrorWriterError on any S3 write failure.
    """
    # LOGIC — build predictable manifest key (no timestamp so it is overwritten each run)
    manifest_key = f"manifests/{desk_code}_{trade_date_str}_errors_manifest.json"

    # LOGIC — ISO 8601 timestamp with ET UTC offset, e.g. "2026-06-15T19:32:11-04:00"
    generated_at_et = now_et.isoformat()

    manifest_payload = {
        "error_file_key": error_key,
        "generated_at_et": generated_at_et,
        "row_count": row_count,
    }
    manifest_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")

    logger.info(
        "Writing error manifest to s3://%s/%s (points to %s, %d rows)",
        bucket,
        manifest_key,
        error_key,
        row_count,
    )

    # LOGIC — upload manifest to S3; overwrites any previous manifest for this desk/date
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=manifest_bytes,
            ContentType="application/json; charset=utf-8",
        )
    except Exception as exc:
        logger.error(
            "Failed to write error manifest to s3://%s/%s: %s",
            bucket,
            manifest_key,
            exc,
        )
        raise ErrorWriterError(
            f"Failed to write error manifest to s3://{bucket}/{manifest_key}: {exc}"
        ) from exc

    logger.info(
        "Error manifest written successfully: s3://%s/%s", bucket, manifest_key
    )