# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)


class _ReportEncoder(json.JSONEncoder):
    # BOILERPLATE — handles types that are valid in the report dict but not natively JSON-serialisable
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, "isoformat"):
            # datetime.date and datetime.datetime
            return obj.isoformat()
        return super().default(obj)


def _format_timestamp_for_key(processing_timestamp_et: str) -> str:
    # LOGIC — converts ISO 8601 ET string to YYYYMMDDTHHMMSS for use in S3 keys
    # The ISO string may include timezone offset, e.g. "2024-06-15T13:45:22.123456-04:00"
    # We parse and reformat to strip microseconds and offset, keeping only the local time part.
    try:
        et_tz = pytz.timezone("America/Toronto")
        # fromisoformat handles offsets in Python 3.7+
        dt = datetime.fromisoformat(processing_timestamp_et)
        # Normalise to ET in case the string carries a different offset
        if dt.tzinfo is None:
            dt = et_tz.localize(dt)
        else:
            dt = dt.astimezone(et_tz)
        return dt.strftime("%Y%m%dT%H%M%S")
    except (ValueError, AttributeError) as exc:
        # LOGIC — fall back to stripping characters if parsing fails; log warning
        logger.warning(
            "Could not parse processing_timestamp_et '%s' for key formatting: %s — using sanitised fallback",
            processing_timestamp_et,
            exc,
        )
        # Remove non-alphanumeric except T to build a safe key segment
        safe = "".join(c for c in processing_timestamp_et if c.isdigit() or c == "T")
        return safe[:15] if len(safe) >= 15 else safe


def _get_s3_client():
    # BOILERPLATE — returns a boto3 S3 client; isolated for testability
    return boto3.client("s3")


def write_report(
    report: dict,
    desk_code: str,
    trade_date: str,
    processing_timestamp_et: str,
) -> str:
    # LOGIC — satisfies BAC-4, BAC-7, TAC-4, TAC-5; writes report JSON and manifest to S3
    bucket = os.environ["S3_BUCKET"]
    timestamp_key = _format_timestamp_for_key(processing_timestamp_et)

    # LOGIC — S3 key patterns from data contract
    report_key = f"reports/{desk_code}_{trade_date}_report_{timestamp_key}.json"
    error_key = f"errors/{desk_code}_{trade_date}_errors_{timestamp_key}.csv"
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    report_body = json.dumps(report, indent=2, cls=_ReportEncoder)

    s3 = _get_s3_client()

    # LOGIC — write timestamped report JSON
    logger.info("Writing report JSON to s3://%s/%s", bucket, report_key)
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Report JSON written successfully: %s", report_key)

    # LOGIC — build manifest content (predictable key, always overwritten)
    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "error_key": error_key,
        "generated_at_et": processing_timestamp_et,
    }
    manifest_body = json.dumps(manifest, indent=2)

    # LOGIC — write manifest at predictable key so downstream consumers can find latest files
    logger.info("Writing manifest JSON to s3://%s/%s", bucket, manifest_key)
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest JSON written successfully: %s", manifest_key)

    return report_key