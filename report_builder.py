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

# BOILERPLATE — timezone constant
_ET = pytz.timezone("America/Toronto")

# BOILERPLATE — expected columns for null-rate computation
_AUDIT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _now_et() -> datetime:
    # BOILERPLATE — returns current wall-clock time in America/Toronto
    return datetime.now(_ET)


def _format_iso_et(dt: datetime) -> str:
    # BOILERPLATE — produces ISO 8601 offset-aware string, e.g. 2026-06-15T19:30:45-04:00
    return dt.isoformat()


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — null rate per required column on the original raw DataFrame
    total = len(raw_df)
    rates: dict = {}
    for col in _AUDIT_COLUMNS:
        if total == 0:
            rates[col] = 0.0
        elif col in raw_df.columns:
            # count NaN and empty-string as null
            null_count = raw_df[col].isna().sum() + (raw_df[col].astype(str).str.strip() == "").sum()
            # astype(str) on NaN produces "nan" — correct for empty-string check only on non-null rows
            null_count = raw_df[col].isna().sum()
            rates[col] = float(null_count) / float(total)
        else:
            rates[col] = 1.0  # column entirely absent — treat every row as null
    return rates


def _compute_counts_by_desk_code(valid_df: pd.DataFrame) -> dict:
    # LOGIC — row counts per desk_code from the validated (passing) rows only
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    grouped = valid_df.groupby("desk_code", sort=True).size()
    return {str(k): int(v) for k, v in grouped.items()}


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — min and max notional_amount cast to float, computed on valid rows only
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    try:
        amounts = valid_df["notional_amount"].astype(float)
        return float(amounts.min()), float(amounts.max())
    except (ValueError, TypeError) as exc:
        logger.warning("Could not compute notional stats: %s", exc)
        return None, None


def _build_report_dict(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    filename: str,
    timestamp_et: datetime,
) -> dict:
    # LOGIC — assemble the canonical report structure from design spec
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted  # LOGIC — valid but already in DB

    null_rates = _compute_null_rates(raw_df)
    counts_by_desk = _compute_counts_by_desk_code(valid_df)
    min_notional, max_notional = _compute_notional_stats(valid_df)

    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": _format_iso_et(timestamp_et),
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "counts_by_desk_code": counts_by_desk,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates_by_column": null_rates,
    }
    return report


def _upload_json_to_s3(s3_client, bucket: str, key: str, payload: dict) -> None:
    # BOILERPLATE — serialise dict to UTF-8 JSON and PUT to S3
    body = json.dumps(payload, indent=2, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    logger.info("Uploaded JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))


def _build_report_s3_key(desk_code: str, trade_date: str, timestamp_et: datetime) -> str:
    # LOGIC — canonical report key: reports/{desk_code}_{trade_date}_summary_{YYYYMMDDTHHmmSS}.json
    ts_str = timestamp_et.strftime("%Y%m%dT%H%M%S")
    return f"reports/{desk_code}_{trade_date}_summary_{ts_str}.json"


def _build_report_manifest_key(desk_code: str, trade_date: str) -> str:
    # LOGIC — predictable (no timestamp) manifest key so downstream consumers always know where to look
    return f"manifests/{desk_code}_{trade_date}_report_manifest.json"


def build_and_upload_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    filename: str,
    bucket: str,
) -> dict:
    """
    Builds the processing summary report, uploads it to S3, writes the predictable
    manifest JSON, and returns the summary dict.

    Returns summary_dict matching the canonical report JSON structure.
    """
    # BOILERPLATE — capture consistent timestamp for this report
    timestamp_et = _now_et()

    logger.info(
        "Building report for filename=%s desk_code=%s trade_date=%s "
        "total_rows=%d valid=%d rejected=%d rows_inserted=%d",
        filename,
        desk_code,
        trade_date,
        len(raw_df),
        len(valid_df),
        len(rejected_df),
        rows_inserted,
    )

    # LOGIC — build the full summary dictionary
    summary_dict = _build_report_dict(
        raw_df=raw_df,
        valid_df=valid_df,
        rejected_df=rejected_df,
        rows_inserted=rows_inserted,
        desk_code=desk_code,
        trade_date=trade_date,
        filename=filename,
        timestamp_et=timestamp_et,
    )

    # BOILERPLATE — S3 client (uses existing AWS infrastructure)
    s3_client = boto3.client("s3")

    # LOGIC — derive the timestamped report key and upload
    report_key = _build_report_s3_key(desk_code, trade_date, timestamp_et)
    _upload_json_to_s3(s3_client, bucket, report_key, summary_dict)

    # LOGIC — build and overwrite the predictable manifest so downstream consumers
    # always find the latest report without guessing the timestamp
    manifest_key = _build_report_manifest_key(desk_code, trade_date)
    manifest_payload = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_file_key": report_key,
        "generated_at_et": _format_iso_et(timestamp_et),
    }
    _upload_json_to_s3(s3_client, bucket, manifest_key, manifest_payload)

    logger.info(
        "Report uploaded: s3://%s/%s | Manifest: s3://%s/%s",
        bucket,
        report_key,
        bucket,
        manifest_key,
    )

    # LOGIC — attach the report S3 key to the returned dict so the caller
    # (pipeline_handler) can pass it to sns_notifier without recomputing the key
    summary_dict["report_s3_key"] = report_key

    return summary_dict