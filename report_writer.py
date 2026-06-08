# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_et_timestamp_str() -> str:
    # LOGIC — current time in America/Toronto formatted as yyyymmddHHMMSS for filename use
    et_tz = pytz.timezone("America/Toronto")
    return datetime.now(et_tz).strftime("%Y%m%d%H%M%S")


def _get_et_iso_str() -> str:
    # LOGIC — current time in America/Toronto as ISO 8601 string for manifest metadata
    et_tz = pytz.timezone("America/Toronto")
    return datetime.now(et_tz).strftime("%Y-%m-%dT%H:%M:%S%z")


def _build_report_key(source_filename: str, timestamp_str: str) -> str:
    # LOGIC — derive report S3 key from source filename using data contract pattern:
    # reports/{desk_code}_{trade_date}_positions_report_{yyyymmddHHMMSS}.json
    # source_filename example: EQDESK_2026-06-01_positions.csv
    if source_filename.lower().endswith(".csv"):
        base = source_filename[: -len(".csv")]
    else:
        base = source_filename
    return f"reports/{base}_report_{timestamp_str}.json"


def _build_manifest_key(desk_code: str, trade_date: str) -> str:
    # LOGIC — predictable manifest key (no timestamp) so downstream consumers can find it:
    # manifests/{desk_code}_{trade_date}_manifest.json
    return f"manifests/{desk_code}_{trade_date}_manifest.json"


def write_report_to_s3(
    report: dict,
    source_filename: str,
    error_key: "str | None",
    bucket: str,
) -> str:
    # BOILERPLATE — S3 client; bucket comes from caller (lambda_handler passes os.environ["S3_BUCKET"])
    s3_client = boto3.client("s3")

    # LOGIC — generate timestamp once so report key and manifest share the same ET instant
    timestamp_str: str = _get_et_timestamp_str()

    # LOGIC — construct report S3 key following data contract pattern
    report_key: str = _build_report_key(source_filename, timestamp_str)

    # LOGIC — serialise report dict to JSON; default=str handles Decimal, date, and other types
    report_json: str = json.dumps(report, default=str)

    logger.info("Writing report to s3://%s/%s", bucket, report_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Report written successfully to s3://%s/%s", bucket, report_key)

    # LOGIC — extract desk_code and trade_date from the report dict (set by report_builder)
    # These are guaranteed present by build_report(); no need to re-parse the filename
    desk_code: str = report.get("desk_code", "UNKNOWN")
    trade_date: str = report.get("trade_date", "UNKNOWN")

    # LOGIC — build predictable manifest key (overwritten on reprocessing per data contract)
    manifest_key: str = _build_manifest_key(desk_code, trade_date)

    # LOGIC — manifest content matches S3 Manifest JSON Schema in data contract exactly
    manifest: dict = {
        "source_filename": source_filename,
        "report_key": report_key,
        "error_key": error_key,  # None becomes JSON null via json.dumps
        "generated_at_et": _get_et_iso_str(),
    }
    manifest_json: str = json.dumps(manifest, default=str)

    logger.info("Writing manifest to s3://%s/%s", bucket, manifest_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_json.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest written successfully to s3://%s/%s", bucket, manifest_key)

    return report_key