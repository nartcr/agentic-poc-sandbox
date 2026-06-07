import io
import json
import logging
import os

import boto3
import pytz

from datetime import datetime

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def _now_et() -> datetime:
    # LOGIC — produce current wall-clock time as ET-aware datetime
    return datetime.now(_ET)


def write_error_file(
    rejected_df,
    bucket: str,
    desk_code: str,
    trade_date: str,
) -> str:
    """
    Serialize rejected_df to CSV and upload to S3 under errors/.
    Write a manifest JSON to manifests/ pointing at the error file.
    Returns the S3 key of the written error CSV.
    """
    # LOGIC — capture a single consistent timestamp for both artefacts
    now_et: datetime = _now_et()
    timestamp_str: str = now_et.strftime("%Y%m%dT%H%M%S")
    generated_at_iso: str = now_et.isoformat()

    # LOGIC — build S3 keys per DATA CONTRACTS
    error_key: str = (
        f"errors/{desk_code}_{trade_date}_positions_errors_{timestamp_str}.csv"
    )
    manifest_key: str = (
        f"manifests/{desk_code}_{trade_date}_errors_manifest.json"
    )

    # LOGIC — serialize rejected_df to CSV in memory (no /tmp/)
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes: bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — S3 client (instantiated at call time, not module level)
    s3_client = boto3.client("s3")

    # LOGIC — upload error CSV
    logger.info(
        "Uploading error file to s3://%s/%s (%d bytes, %d rejected rows)",
        bucket,
        error_key,
        len(csv_bytes),
        len(rejected_df),
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info("Error file uploaded: %s", error_key)

    # LOGIC — build manifest payload per DATA CONTRACTS
    manifest_payload: dict = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_file_key": error_key,
        "generated_at_et": generated_at_iso,
    }
    manifest_bytes: bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")

    # LOGIC — upload manifest (overwrite so it always points to latest run)
    logger.info(
        "Uploading error manifest to s3://%s/%s",
        bucket,
        manifest_key,
    )
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )
    logger.info("Error manifest uploaded: %s", manifest_key)

    return error_key