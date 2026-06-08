# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — original input columns (used when computing null_rates over all rows)
_INPUT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _decimal_to_float(value) -> float:
    # LOGIC — safely convert Decimal (used in valid_df) to float for JSON serialisation
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def _compute_null_rates(valid_df: pd.DataFrame, rejected_df: pd.DataFrame) -> dict:
    # LOGIC — compute per-column null rate over ALL rows (valid + rejected),
    # original columns only (rejection_reason excluded).
    # An empty string is treated as present (not null) — consistent with validator behaviour.

    all_frames = []
    if not valid_df.empty:
        # LOGIC — valid_df may have trade_date as date objects and notional_amount as Decimal;
        # we only need presence/absence, so cast everything to str for uniform treatment.
        all_frames.append(valid_df[[c for c in _INPUT_COLUMNS if c in valid_df.columns]].astype(str))
    if not rejected_df.empty:
        rej_cols = [c for c in _INPUT_COLUMNS if c in rejected_df.columns]
        all_frames.append(rejected_df[rej_cols].astype(str))

    if not all_frames:
        # LOGIC — no rows at all; every column has null_rate 0.0 by convention
        return {col: 0.0 for col in _INPUT_COLUMNS}

    combined = pd.concat(all_frames, ignore_index=True)
    total = len(combined)

    null_rates: dict = {}
    for col in _INPUT_COLUMNS:
        if col not in combined.columns:
            null_rates[col] = 0.0
            continue
        # LOGIC — a cell counts as "null" only if it is a genuine NaN/None;
        # empty string "" is not counted as null here (validator already rejected those rows).
        null_count = combined[col].isna().sum()
        null_rates[col] = round(null_count / total, 6) if total > 0 else 0.0

    return null_rates


def _compute_notional_stats(valid_df: pd.DataFrame):
    # LOGIC — returns (notional_min, notional_max) as float or None when valid_df is empty
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    amounts = valid_df["notional_amount"].apply(_decimal_to_float)
    return float(amounts.min()), float(amounts.max())


def _compute_desk_code_counts(valid_df: pd.DataFrame) -> dict:
    # LOGIC — group valid rows by desk_code and count; returns {} when valid_df is empty
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    counts = valid_df["desk_code"].value_counts().to_dict()
    # LOGIC — ensure keys are plain str (not numpy types)
    return {str(k): int(v) for k, v in counts.items()}


def build_and_write_report(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    processing_timestamp_et: datetime,
) -> str:
    # BOILERPLATE
    s3_bucket = os.environ["S3_BUCKET"]
    report_key = f"reports/{desk_code}_{trade_date}_report.json"
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"
    error_key = f"errors/{desk_code}_{trade_date}_errors.csv"

    # LOGIC — derive summary counts
    total_rows = len(valid_df) + len(rejected_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — compute derived statistics
    notional_min, notional_max = _compute_notional_stats(valid_df)
    desk_code_counts = _compute_desk_code_counts(valid_df)
    null_rates = _compute_null_rates(valid_df, rejected_df)

    # LOGIC — serialise processing_timestamp_et to ISO-8601 with ET UTC offset
    timestamp_str = processing_timestamp_et.isoformat()

    # LOGIC — assemble the report dict using the exact field names from the data contract
    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": timestamp_str,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    report_json_bytes = json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8")

    # BOILERPLATE — write report JSON to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=report_key,
        Body=report_json_bytes,
        ContentType="application/json; charset=utf-8",
    )
    logger.info("Report written: s3://%s/%s", s3_bucket, report_key)

    # LOGIC — assemble the manifest using the exact schema from the data contract
    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "error_key": error_key,
        "generated_at_et": timestamp_str,
    }

    manifest_json_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")

    # BOILERPLATE — write manifest JSON to S3; overwrite on reprocessing intentionally
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=manifest_key,
        Body=manifest_json_bytes,
        ContentType="application/json; charset=utf-8",
    )
    logger.info("Manifest written: s3://%s/%s", s3_bucket, manifest_key)

    # LOGIC — return the report S3 key so pipeline_handler can include it in the SNS message
    return report_key