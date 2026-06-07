# BOILERPLATE
import io
import json
import logging
import os

import boto3
import pytz
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def _get_et_timestamp() -> str:
    # LOGIC — returns current time as ISO-8601 string in America/Toronto
    return datetime.now(_ET).isoformat()


def write_rejected_rows(bucket: str, desk_code: str, trade_date: str, rejected_df) -> str:
    """
    # LOGIC — Serialize rejected_df to CSV (including rejection_reason column)
    and write to s3://{bucket}/errors/{desk_code}_{trade_date}_rejected.csv.
    Returns the S3 key written.
    Idempotent: overwrites on re-processing.
    """
    # LOGIC — build the S3 key from the pattern in the data contracts
    s3_key = f"errors/{desk_code}_{trade_date}_rejected.csv"

    # LOGIC — serialize DataFrame to CSV in-memory; no /tmp/ path used
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3 using put_object (overwrites existing object)
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Wrote rejected rows CSV to s3://%s/%s (%d rows, %d bytes)",
        bucket,
        s3_key,
        len(rejected_df),
        len(csv_bytes),
    )
    return s3_key


def write_report_json(bucket: str, desk_code: str, trade_date: str, report: dict) -> str:
    """
    # LOGIC — Serialize report dict to JSON and write to
    s3://{bucket}/reports/{desk_code}_{trade_date}_summary.json.
    Returns the S3 key written.
    Idempotent: overwrites on re-processing.
    """
    # LOGIC — build the S3 key from the pattern in the data contracts
    s3_key = f"reports/{desk_code}_{trade_date}_summary.json"

    # LOGIC — serialize report dict to JSON bytes
    report_bytes = json.dumps(report, indent=2, default=str).encode("utf-8")

    # BOILERPLATE — write to S3 using put_object (overwrites existing object)
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Wrote summary report JSON to s3://%s/%s (%d bytes)",
        bucket,
        s3_key,
        len(report_bytes),
    )
    return s3_key


def write_manifest(bucket: str, desk_code: str, trade_date: str, keys: dict) -> str:
    """
    # LOGIC — Write a manifest JSON to
    s3://{bucket}/manifests/{desk_code}_{trade_date}_manifest.json.
    The manifest maps logical output names (e.g. 'report', 'errors') to
    actual S3 keys so downstream consumers can locate outputs at a
    predictable key without guessing timestamps.
    Returns the S3 key written.
    Idempotent: overwrites on re-processing so it always reflects the
    latest run.
    """
    # LOGIC — build the S3 key from the pattern in the data contracts
    s3_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    # LOGIC — assemble manifest payload matching the manifest JSON schema
    # in the data contracts
    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "generated_at_et": _get_et_timestamp(),
        "files": {
            logical_name: actual_key
            for logical_name, actual_key in keys.items()
        },
    }

    # LOGIC — serialize manifest to JSON bytes
    manifest_bytes = json.dumps(manifest, indent=2, default=str).encode("utf-8")

    # BOILERPLATE — write to S3 using put_object (overwrites existing object)
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Wrote manifest JSON to s3://%s/%s (%d bytes)",
        bucket,
        s3_key,
        len(manifest_bytes),
    )
    return s3_key