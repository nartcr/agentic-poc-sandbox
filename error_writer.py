# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — module-level S3 client (reused across invocations)
_s3_client = None


def _get_s3_client():
    # BOILERPLATE
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _now_et() -> datetime:
    # LOGIC — always produce an ET-aware datetime
    tz_et = pytz.timezone("America/Toronto")
    return datetime.now(tz_et)


def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    bucket: str,
) -> "str | None":
    """
    Write rejected rows to S3 under errors/ prefix and write a manifest
    under manifests/.  Returns the S3 key of the error CSV, or None if
    rejected_df is empty.
    """
    # LOGIC — nothing to write when there are no rejected rows
    if rejected_df is None or rejected_df.empty:
        logger.info(
            "No rejected rows for desk_code=%s trade_date=%s; skipping error file write.",
            desk_code,
            trade_date,
        )
        return None

    s3 = _get_s3_client()
    now_et = _now_et()

    # LOGIC — build timestamp suffix in ET
    timestamp_suffix = now_et.strftime("%Y%m%d_%H%M%S")

    # LOGIC — construct S3 key exactly as specified in data contracts
    error_key = (
        f"errors/{desk_code}_{trade_date}_positions_errors_{timestamp_suffix}.csv"
    )

    # LOGIC — serialize rejected_df to CSV in memory; no filesystem writes
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    logger.info(
        "Writing %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        error_key,
    )
    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info("Error file written: s3://%s/%s", bucket, error_key)

    # LOGIC — build and write manifest so downstream consumers can find the file
    source_file = f"{desk_code}_{trade_date}_positions.csv"
    generated_at_et_str = now_et.isoformat()

    manifest = {
        "source_file": source_file,
        "error_file_key": error_key,
        "generated_at_et": generated_at_et_str,
        "row_count": len(rejected_df),
    }

    manifest_key = f"manifests/{desk_code}_{trade_date}_errors_manifest.json"
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")

    logger.info(
        "Writing error manifest to s3://%s/%s",
        bucket,
        manifest_key,
    )
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )
    logger.info("Error manifest written: s3://%s/%s", bucket, manifest_key)

    return error_key