# BOILERPLATE
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET_TZ = pytz.timezone("America/Toronto")

# Columns tracked for null-rate computation (all seven logical CSV columns)
_NULL_RATE_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _format_processing_ts(processing_ts_et: datetime) -> str:
    # LOGIC — format timestamp as YYYYMMDDHHmmSS for S3 key suffix
    return processing_ts_et.strftime("%Y%m%d%H%M%S")


def _format_iso_et(processing_ts_et: datetime) -> str:
    # LOGIC — format timestamp as ISO 8601 with ET offset for JSON fields
    return processing_ts_et.isoformat()


def _build_report_s3_key(desk_code: str, trade_date_str: str, processing_ts_et: datetime) -> str:
    # LOGIC — construct reports/ key per data contract:
    # reports/{desk_code}_{trade_date}_report_{YYYYMMDDHHmmSS}.json
    ts_str = _format_processing_ts(processing_ts_et)
    return f"reports/{desk_code}_{trade_date_str}_report_{ts_str}.json"


def _build_report_manifest_key(desk_code: str, trade_date_str: str) -> str:
    # LOGIC — predictable manifest key (no timestamp) per S3 manifest pattern
    return f"manifests/{desk_code}_{trade_date_str}_report_manifest.json"


def _is_null_value(value) -> bool:
    # LOGIC — a field counts as null if it is None, missing, or empty string
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _compute_null_rates(all_rows: list) -> dict:
    # LOGIC — compute per-column null rate over all rows (valid + rejected combined)
    # null_rate = null_count / total_rows_received per TAC-4
    total = len(all_rows)
    if total == 0:
        return {col: 0.0 for col in _NULL_RATE_COLUMNS}

    null_counts = {col: 0 for col in _NULL_RATE_COLUMNS}
    for row in all_rows:
        for col in _NULL_RATE_COLUMNS:
            if _is_null_value(row.get(col)):
                null_counts[col] += 1

    return {col: round(null_counts[col] / total, 6) for col in _NULL_RATE_COLUMNS}


def _compute_notional_stats(valid_rows: list) -> tuple:
    # LOGIC — return (min_notional, max_notional) as Decimal from valid rows
    # Returns ("0", "0") sentinel strings when there are no valid rows
    if not valid_rows:
        return (Decimal("0"), Decimal("0"))

    notionals = []
    for row in valid_rows:
        raw = row.get("notional_amount")
        if raw is None:
            continue
        if isinstance(raw, Decimal):
            notionals.append(raw)
        else:
            # LOGIC — defensive: attempt conversion if not already Decimal
            try:
                notionals.append(Decimal(str(raw)))
            except InvalidOperation:
                logger.warning("Could not convert notional_amount to Decimal: %r", raw)
                continue

    if not notionals:
        return (Decimal("0"), Decimal("0"))

    return (min(notionals), max(notionals))


def _compute_grouped_by_desk_code(valid_rows: list) -> dict:
    # LOGIC — count valid rows grouped by desk_code value
    grouped = {}
    for row in valid_rows:
        dc = row.get("desk_code", "")
        if dc is None:
            dc = ""
        dc = str(dc)
        grouped[dc] = grouped.get(dc, 0) + 1
    return grouped


def _format_notional(value: Decimal) -> str:
    # LOGIC — render Decimal to 4 decimal places string for JSON output
    return f"{value:.4f}"


def _build_report_dict(
    valid_rows: list,
    rejected_rows: list,
    desk_code: str,
    trade_date_str: str,
    rows_inserted: int,
    processing_ts_et: datetime,
    error_file_key: str,
) -> dict:
    # LOGIC — assemble the full report JSON structure per the data contract
    total_rows_received = len(valid_rows) + len(rejected_rows)
    rows_rejected = len(rejected_rows)
    rows_skipped_duplicate = len(valid_rows) - rows_inserted  # TAC-4

    all_rows = list(valid_rows) + list(rejected_rows)
    null_rates = _compute_null_rates(all_rows)
    min_notional, max_notional = _compute_notional_stats(valid_rows)
    grouped_by_desk_code = _compute_grouped_by_desk_code(valid_rows)

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "processing_timestamp_et": _format_iso_et(processing_ts_et),
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "grouped_by_desk_code": grouped_by_desk_code,
        "min_notional_amount": _format_notional(min_notional),
        "max_notional_amount": _format_notional(max_notional),
        "null_rates": null_rates,
        "error_file_key": error_file_key,
    }
    return report


def _write_s3_object(s3_client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    # BOILERPLATE — put object to S3 with explicit content type
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )
    logger.info("Wrote S3 object: s3://%s/%s (%d bytes)", bucket, key, len(body))


def write_report(
    s3_client,
    bucket: str,
    valid_rows: list,
    rejected_rows: list,
    desk_code: str,
    trade_date_str: str,
    rows_inserted: int,
    processing_ts_et: datetime,
    error_file_key: str = "",
) -> tuple:
    # LOGIC — main entry point; builds report dict, writes JSON to S3, writes manifest
    # Returns: (s3_key, report_dict)
    report_key = _build_report_s3_key(desk_code, trade_date_str, processing_ts_et)
    manifest_key = _build_report_manifest_key(desk_code, trade_date_str)

    report_dict = _build_report_dict(
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        desk_code=desk_code,
        trade_date_str=trade_date_str,
        rows_inserted=rows_inserted,
        processing_ts_et=processing_ts_et,
        error_file_key=error_file_key,
    )

    # Serialize with Decimal-safe encoder
    report_bytes = json.dumps(report_dict, indent=2, default=str).encode("utf-8")
    _write_s3_object(s3_client, bucket, report_key, report_bytes, "application/json")
    logger.info(
        "Report written: key=%s, total_rows=%d, rows_loaded=%d, rows_rejected=%d",
        report_key,
        report_dict["total_rows_received"],
        report_dict["rows_loaded"],
        report_dict["rows_rejected"],
    )

    # Write manifest pointing to the actual report file key
    manifest_payload = {"report_file": report_key}
    manifest_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")
    _write_s3_object(s3_client, bucket, manifest_key, manifest_bytes, "application/json")
    logger.info("Report manifest written: key=%s -> %s", manifest_key, report_key)

    return (report_key, report_dict)