# BOILERPLATE
import csv
import io
import json
import logging
from datetime import datetime
from decimal import Decimal

import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET_TZ = pytz.timezone("America/Toronto")

# Error CSV columns in the required order per the data contract
_ERROR_CSV_COLUMNS = [
    "row_number",
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def _format_processing_ts(processing_ts_et: datetime) -> str:
    # LOGIC — format timestamp as YYYYMMDDHHmmSS for use in S3 key
    return processing_ts_et.strftime("%Y%m%d%H%M%S")


def _build_error_s3_key(desk_code: str, trade_date_str: str, processing_ts_et: datetime) -> str:
    # LOGIC — construct the errors/ key per data contract:
    # errors/{desk_code}_{trade_date}_errors_{YYYYMMDDHHmmSS}.csv
    ts_str = _format_processing_ts(processing_ts_et)
    return f"errors/{desk_code}_{trade_date_str}_errors_{ts_str}.csv"


def _build_error_manifest_key(desk_code: str, trade_date_str: str) -> str:
    # LOGIC — manifest key is predictable (no timestamp) per S3 manifest pattern
    return f"manifests/{desk_code}_{trade_date_str}_errors_manifest.json"


def _serialize_row_for_csv(row: dict) -> dict:
    # LOGIC — coerce all values to strings safe for CSV output;
    # Decimal and date objects must be rendered as plain strings
    serialized = {}
    for col in _ERROR_CSV_COLUMNS:
        raw = row.get(col, "")
        if raw is None:
            serialized[col] = ""
        elif isinstance(raw, Decimal):
            serialized[col] = str(raw)
        else:
            serialized[col] = str(raw)
    return serialized


def _build_csv_bytes(rejected_rows: list) -> bytes:
    # LOGIC — write header + all rejected rows into an in-memory buffer
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=_ERROR_CSV_COLUMNS,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rejected_rows:
        writer.writerow(_serialize_row_for_csv(row))
    return buffer.getvalue().encode("utf-8")


def _write_s3_object(s3_client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    # BOILERPLATE — put object to S3 with explicit content type
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )
    logger.info("Wrote S3 object: s3://%s/%s (%d bytes)", bucket, key, len(body))


def write_error_file(
    s3_client,
    bucket: str,
    rejected_rows: list,
    desk_code: str,
    trade_date_str: str,
    processing_ts_et: datetime,
) -> str:
    # LOGIC — main entry point; writes error CSV and manifest, returns the error S3 key
    error_key = _build_error_s3_key(desk_code, trade_date_str, processing_ts_et)
    manifest_key = _build_error_manifest_key(desk_code, trade_date_str)

    # Build and upload the error CSV (header-only if no rejected rows)
    csv_bytes = _build_csv_bytes(rejected_rows)
    _write_s3_object(s3_client, bucket, error_key, csv_bytes, "text/csv")
    logger.info(
        "Error file written: key=%s, rejected_row_count=%d",
        error_key,
        len(rejected_rows),
    )

    # Write manifest pointing to the actual error file key
    manifest_payload = {"error_file": error_key}
    manifest_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")
    _write_s3_object(s3_client, bucket, manifest_key, manifest_bytes, "application/json")
    logger.info("Error manifest written: key=%s -> %s", manifest_key, error_key)

    return error_key