# BOILERPLATE
import io
import json
import logging
import os
import datetime

import boto3
import pandas as pd

from time_utils import format_et, format_et_compact

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — module-level S3 client (reused across invocations within a warm Lambda)
_s3_client = None


def _get_s3_client():
    # BOILERPLATE
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _compute_null_rates(df: pd.DataFrame) -> dict:
    # LOGIC — per-column null rate across all rows; empty-string values are treated as null
    if df.empty:
        columns = [
            "trade_id",
            "desk_code",
            "trade_date",
            "instrument_type",
            "notional_amount",
            "currency",
            "counterparty_id",
        ]
        return {col: 0.0 for col in columns}

    total = len(df)
    null_rates = {}
    for col in df.columns:
        # LOGIC — skip the rejection_reason column if present; it belongs to rejected_df only
        if col == "rejection_reason":
            continue
        # LOGIC — count nulls AND empty strings as missing
        null_count = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        null_rates[col] = round(float(null_count) / float(total), 6)
    return null_rates


def _write_s3_json(bucket: str, key: str, payload: dict) -> None:
    # LOGIC — serialize dict to JSON and write to S3; default=str handles date/datetime objects
    body = json.dumps(payload, indent=2, default=str)
    _get_s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))


def write_report(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: datetime.date,
    filename: str,
    processing_timestamp: datetime.datetime,
    error_s3_key,
) -> tuple:
    """
    Compute summary statistics, write the timestamped report JSON and the
    idempotent manifest JSON to S3.  Returns (summary_dict, report_s3_key).
    """
    # BOILERPLATE
    bucket = os.environ["S3_BUCKET"]
    ts_compact = format_et_compact(processing_timestamp)
    ts_iso = format_et(processing_timestamp)

    # LOGIC — build combined DataFrame for null-rate calculation (valid + rejected)
    frames_for_null = []
    if not valid_df.empty:
        frames_for_null.append(valid_df)
    if not rejected_df.empty:
        # LOGIC — drop rejection_reason before combining so column sets align
        frames_for_null.append(rejected_df.drop(columns=["rejection_reason"], errors="ignore"))

    combined_df = pd.concat(frames_for_null, ignore_index=True) if frames_for_null else pd.DataFrame()

    # LOGIC — compute per-column null rates across all rows
    null_rates = _compute_null_rates(combined_df)

    # LOGIC — desk_code_counts from valid rows only (as specified)
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_code_counts = (
            valid_df.groupby("desk_code", dropna=False)
            .size()
            .to_dict()
        )
        # LOGIC — convert numpy int64 keys/values to native Python types for JSON serialisation
        desk_code_counts = {str(k): int(v) for k, v in desk_code_counts.items()}
    else:
        desk_code_counts = {}

    # LOGIC — notional stats; null when valid_df is empty
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min = float(notional_series.min()) if not notional_series.isna().all() else None
        notional_max = float(notional_series.max()) if not notional_series.isna().all() else None
    else:
        notional_min = None
        notional_max = None

    # LOGIC — row counts per spec: total = valid + rejected, rows_loaded = rows_inserted
    total_rows = len(valid_df) + len(rejected_df)
    rows_rejected = len(rejected_df)

    # LOGIC — assemble summary dict matching the Summary Report JSON schema exactly
    summary = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": str(trade_date),
        "processing_timestamp_et": ts_iso,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    # LOGIC — S3 key: reports/{desk_code}_{trade_date}_report_{yyyymmddTHHMMSS}.json
    report_key = (
        f"reports/{desk_code}_{trade_date}_report_{ts_compact}.json"
    )
    _write_s3_json(bucket, report_key, summary)
    logger.info(
        "Report written for desk_code=%s trade_date=%s: total=%d loaded=%d rejected=%d",
        desk_code,
        trade_date,
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    # LOGIC — manifest at predictable key; overwritten on reprocessing
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"
    manifest = {
        "report_key": report_key,
        "error_key": error_s3_key,  # None serialises to JSON null via default=str; handle explicitly
        "generated_at_et": ts_iso,
    }
    # LOGIC — ensure error_key is JSON null (not the string "None") when absent
    manifest_body = json.dumps(
        {
            "report_key": report_key,
            "error_key": error_s3_key if error_s3_key is not None else None,
            "generated_at_et": ts_iso,
        },
        indent=2,
    )
    _get_s3_client().put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest written to s3://%s/%s", bucket, manifest_key)

    return summary, report_key