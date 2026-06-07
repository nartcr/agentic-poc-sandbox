# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — canonical columns used for null rate computation
_CANONICAL_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — null rate: count of null/empty/whitespace values divided by total rows
    total = len(raw_df)
    null_rates = {}
    for col in _CANONICAL_COLUMNS:
        if col not in raw_df.columns:
            null_rates[col] = 1.0
            continue
        if total == 0:
            null_rates[col] = 0.0
            continue
        null_count = raw_df[col].apply(
            lambda v: v is None or (isinstance(v, str) and v.strip() == "")
        ).sum()
        null_rates[col] = round(int(null_count) / total, 6)
    return null_rates


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — compute min/max of notional_amount from valid rows only
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    decimals = []
    for raw_val in valid_df["notional_amount"]:
        try:
            decimals.append(Decimal(str(raw_val).strip()))
        except InvalidOperation:
            continue
    if not decimals:
        return None, None
    return min(decimals), max(decimals)


def _format_decimal(value) -> str | None:
    # LOGIC — format Decimal to 4 decimal places string, matching NUMERIC(20,4)
    if value is None:
        return None
    return f"{value:.4f}"


def _build_report_dict(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    processing_timestamp_et: datetime,
) -> dict:
    # LOGIC — assemble all report fields per the approved design
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — record counts by desk_code from valid rows only
    if valid_df.empty or "desk_code" not in valid_df.columns:
        record_counts_by_desk_code = {}
    else:
        record_counts_by_desk_code = (
            valid_df.groupby("desk_code").size().to_dict()
        )
        # Convert numpy int64 to plain int for JSON serialisation
        record_counts_by_desk_code = {
            k: int(v) for k, v in record_counts_by_desk_code.items()
        }

    notional_min, notional_max = _compute_notional_stats(valid_df)
    null_rates = _compute_null_rates(raw_df)

    report = {
        "source_file": f"{desk_code}_{trade_date}_positions.csv",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et.isoformat(),
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "record_counts_by_desk_code": record_counts_by_desk_code,
        "notional_amount_min": _format_decimal(notional_min),
        "notional_amount_max": _format_decimal(notional_max),
        "null_rates_by_column": null_rates,
    }
    return report


def _write_s3_json(bucket: str, key: str, payload: dict) -> None:
    # BOILERPLATE — serialize dict to JSON and put to S3
    s3_client = boto3.client("s3")
    body = json.dumps(payload, indent=2, default=str)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Wrote JSON to s3://%s/%s (%d bytes)", bucket, key, len(body))


def build_and_write_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    processing_timestamp_et: datetime,
    bucket: str,
) -> dict:
    """
    Compute the summary report, write it to S3, write the manifest, and return
    the report dict.

    S3 keys written:
      reports/{desk_code}_{trade_date}_summary_{YYYYMMDD_HHMMSS}.json
      manifests/{desk_code}_{trade_date}_report_manifest.json
    """
    # LOGIC — derive timestamp string for the timestamped report key
    ts_str = processing_timestamp_et.strftime("%Y%m%d_%H%M%S")
    report_key = f"reports/{desk_code}_{trade_date}_summary_{ts_str}.json"
    manifest_key = f"manifests/{desk_code}_{trade_date}_report_manifest.json"

    # LOGIC — build the report dict
    report = _build_report_dict(
        raw_df=raw_df,
        valid_df=valid_df,
        rejected_df=rejected_df,
        desk_code=desk_code,
        trade_date=trade_date,
        rows_inserted=rows_inserted,
        processing_timestamp_et=processing_timestamp_et,
    )

    # LOGIC — write the report JSON to S3
    _write_s3_json(bucket, report_key, report)
    logger.info(
        "Summary report written: desk_code=%s trade_date=%s rows_received=%d "
        "rows_inserted=%d rows_rejected=%d rows_skipped=%d",
        desk_code,
        trade_date,
        report["total_rows_received"],
        report["rows_successfully_loaded"],
        report["rows_rejected"],
        report["rows_skipped_duplicate"],
    )

    # LOGIC — write the manifest so downstream consumers can find the report
    # without guessing the timestamp component
    manifest = {
        "source_file": f"{desk_code}_{trade_date}_positions.csv",
        "report_key": report_key,
        "generated_at_et": processing_timestamp_et.isoformat(),
    }
    _write_s3_json(bucket, manifest_key, manifest)
    logger.info("Report manifest written: %s", manifest_key)

    return report