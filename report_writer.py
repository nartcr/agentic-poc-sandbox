# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import pandas as pd

import time_utils

logger = logging.getLogger(__name__)

# LOGIC — mandatory field names for null rate computation (matches data contract)
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — null rate per column: count(null or empty after strip) / total_rows
    total = len(raw_df)
    null_rates = {}
    for col in _MANDATORY_FIELDS:
        if col not in raw_df.columns:
            null_rates[col] = 1.0
            continue
        if total == 0:
            null_rates[col] = 0.0
            continue
        null_count = raw_df[col].apply(
            lambda x: x is None or str(x).strip() == ""
        ).sum()
        null_rates[col] = round(int(null_count) / total, 6)
    return null_rates


def build_report(
    filename: str,
    desk_code: str,
    trade_date_str: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    now_et: datetime,
) -> dict:
    # LOGIC — compute all summary statistics from the provided DataFrames
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)

    # LOGIC — rows_by_desk_code from valid rows only
    if not valid_df.empty:
        rows_by_desk_code = (
            valid_df.groupby("desk_code").size().to_dict()
        )
        # LOGIC — convert numpy int64 to plain int for JSON serialisation
        rows_by_desk_code = {k: int(v) for k, v in rows_by_desk_code.items()}
    else:
        rows_by_desk_code = {}

    # LOGIC — notional stats from valid rows only; None when no valid rows
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_min = float(valid_df["notional_amount"].min())
        notional_max = float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates computed against raw_df (all rows including rejected)
    null_rates = _compute_null_rates(raw_df)

    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "processing_timestamp_et": time_utils.to_et_string(now_et),
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_by_desk_code": rows_by_desk_code,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built: filename=%s total_rows=%d rows_loaded=%d rows_rejected=%d",
        filename,
        total_rows_received,
        rows_inserted,
        rows_rejected,
    )
    return report


def write_report(
    report: dict,
    bucket: str,
    desk_code: str,
    trade_date_str: str,
    s3_client,
    now_et: datetime,
) -> str:
    # LOGIC — S3 key exactly as specified in data contract
    timestamp_suffix = time_utils.et_timestamp_for_key(now_et)
    report_key = (
        f"reports/{desk_code}_{trade_date_str}_positions_report_{timestamp_suffix}.json"
    )

    # LOGIC — serialise report dict to JSON; Decimal values are floats/None after build_report
    report_body = json.dumps(report, indent=2, default=str)

    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, report_key)
    return report_key


def write_report_manifest(
    bucket: str,
    desk_code: str,
    trade_date_str: str,
    report_key: str,
    s3_client,
    now_et: datetime,
) -> None:
    # LOGIC — predictable manifest key (no timestamp) so consumers can locate latest report
    manifest_key = f"manifests/{desk_code}_{trade_date_str}_report_manifest.json"

    manifest = {
        "report_file_key": report_key,
        "generated_at_et": time_utils.to_et_string(now_et),
    }

    manifest_body = json.dumps(manifest, indent=2)

    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Report manifest written to s3://%s/%s", bucket, manifest_key)