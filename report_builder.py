# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pytz

logger = logging.getLogger(__name__)

# LOGIC — mandatory source columns used for null-rate computation
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET_TZ = pytz.timezone("America/Toronto")


def _now_et() -> datetime:
    # LOGIC — current time in America/Toronto, timezone-aware
    return datetime.now(_ET_TZ)


def _decimal_safe_float(value) -> float | None:
    # LOGIC — safely convert Decimal or numeric to float; return None for non-finite or empty
    if value is None:
        return None
    try:
        result = float(value)
        import math
        if not math.isfinite(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _compute_null_rates(valid_df, rejected_df, total_rows: int) -> dict:
    # LOGIC — compute null rates across the combined dataset for each mandatory column
    import pandas as pd

    if total_rows == 0:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    # Reconstruct combined frame from the two halves, keeping only mandatory columns
    frames = []
    for df in (valid_df, rejected_df):
        if not df.empty:
            # Only include mandatory columns that exist in this dataframe
            available = [c for c in _MANDATORY_COLUMNS if c in df.columns]
            if available:
                frames.append(df[available])

    if not frames:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    combined = pd.concat(frames, axis=0, ignore_index=True, sort=False)

    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col not in combined.columns:
            null_rates[col] = 0.0
            continue
        null_count = combined[col].isna().sum()
        # Also count empty-string or whitespace-only as null (mirrors validator logic)
        str_null_count = (
            combined[col]
            .astype(str)
            .str.strip()
            .eq("")
            .sum()
        )
        # Take the max of pandas NA count and whitespace-empty count
        effective_null_count = max(int(null_count), int(str_null_count))
        null_rates[col] = round(effective_null_count / total_rows, 4)

    return null_rates


def _rows_by_desk_code(valid_df) -> dict:
    # LOGIC — group valid rows by desk_code and return count per desk
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    return {str(k): int(v) for k, v in valid_df.groupby("desk_code").size().to_dict().items()}


def _serialize_summary(summary_dict: dict) -> str:
    # LOGIC — JSON-serialize the summary dict; handle Decimal and date types
    import datetime as dt

    def default_serializer(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (dt.date, dt.datetime)):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(summary_dict, indent=2, default=default_serializer)


def build_and_save_report(
    valid_df,
    rejected_df,
    filename: str,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
) -> dict:
    # BOILERPLATE — resolve S3 client and bucket
    bucket = os.environ["S3_BUCKET"]
    s3_client = boto3.client("s3")

    # LOGIC — compute processing timestamp in ET
    now_et = _now_et()
    processing_timestamp_et_iso = now_et.isoformat()
    processing_timestamp_compact = now_et.strftime("%Y%m%dT%H%M%S")

    # LOGIC — compute row counts
    total_rows = len(valid_df) + len(rejected_df)
    rows_rejected = len(rejected_df)

    # LOGIC — compute notional stats from valid_df
    min_notional = None
    max_notional = None
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        try:
            min_notional = _decimal_safe_float(valid_df["notional_amount"].min())
            max_notional = _decimal_safe_float(valid_df["notional_amount"].max())
        except (TypeError, ValueError) as exc:
            logger.warning("Could not compute notional stats: %s", exc)

    # LOGIC — compute null rates across combined dataset
    null_rates = _compute_null_rates(valid_df, rejected_df, total_rows)

    # LOGIC — compute rows_by_desk_code from valid_df
    rows_by_desk_code = _rows_by_desk_code(valid_df)

    # LOGIC — build S3 keys
    report_key = f"reports/{desk_code}_{trade_date}_{processing_timestamp_compact}.json"
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    # LOGIC — assemble summary dict
    summary_dict = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et_iso,
        "rows_by_desk_code": rows_by_desk_code,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
        "report_s3_key": report_key,
    }

    # LOGIC — write report JSON to S3
    report_body = _serialize_summary(summary_dict)
    logger.info("Writing report to s3://%s/%s", bucket, report_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Report written successfully: %s", report_key)

    # LOGIC — assemble and write manifest JSON to predictable key
    manifest_dict = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "manifest_updated_at": processing_timestamp_et_iso,
    }
    manifest_body = json.dumps(manifest_dict, indent=2)
    logger.info("Writing manifest to s3://%s/%s", bucket, manifest_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest written successfully: %s", manifest_key)

    return summary_dict