# BOILERPLATE
import io
import json
import logging
import math
from datetime import datetime
from decimal import Decimal
from typing import Optional

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


# LOGIC — custom JSON encoder to handle Decimal, NaN, and Infinity safely
class _SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

    def iterencode(self, obj, _one_shot=False):
        # Replace NaN/Inf floats with None before encoding
        return super().iterencode(_sanitise(obj), _one_shot=_one_shot)


def _sanitise(obj):
    # LOGIC — recursively replace float NaN/Inf with None for safe JSON output
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, Decimal):
        v = float(obj)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitise(i) for i in obj]
    return obj


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_loaded: int,
    processed_at_et: datetime,
    desk_code: str,
    trade_date: str,
    source_file_key: str,
) -> dict:
    # LOGIC — compute all summary metrics per DATA CONTRACTS report schema
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_loaded

    # LOGIC — notional min/max from valid rows; None when no valid rows exist
    if len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min_val = notional_series.min()
        notional_max_val = notional_series.max()
        notional_min: Optional[float] = (
            None if pd.isna(notional_min_val) else float(notional_min_val)
        )
        notional_max: Optional[float] = (
            None if pd.isna(notional_max_val) else float(notional_max_val)
        )
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates across all columns of raw_df (empty string treated as null
    # because s3_reader ingests all fields as dtype=str)
    null_rates: dict = {}
    if len(raw_df) > 0:
        for col in raw_df.columns:
            null_mask = raw_df[col].isna() | (raw_df[col].astype(str).str.strip() == "")
            null_rates[col] = float(null_mask.sum()) / len(raw_df)
    else:
        for col in raw_df.columns:
            null_rates[col] = 0.0

    # LOGIC — row count by desk_code from valid_df (generic groupby per design)
    if len(valid_df) > 0 and "desk_code" in valid_df.columns:
        row_count_by_desk = (
            valid_df.groupby("desk_code", sort=True)
            .size()
            .to_dict()
        )
        # cast keys to str and values to int for clean JSON serialisation
        row_count_by_desk = {str(k): int(v) for k, v in row_count_by_desk.items()}
    else:
        row_count_by_desk = {}

    # BOILERPLATE — ensure processed_at_et is ET-aware
    if processed_at_et.tzinfo is None:
        processed_at_et = _ET.localize(processed_at_et)

    report_dict = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file_key": source_file_key,
        "processed_at_et": processed_at_et.isoformat(),
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "row_count_by_desk": row_count_by_desk,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built — desk=%s trade_date=%s total=%d loaded=%d rejected=%d skipped=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_duplicate,
    )
    return report_dict


def write_report_to_s3(
    report_dict: dict,
    bucket: str,
    processed_at_et: datetime,
) -> str:
    # LOGIC — derive S3 key from report contents per DATA CONTRACTS pattern:
    # reports/{desk_code}_{trade_date}_positions_report_{YYYYMMDDTHHMMSS}.json
    desk_code = report_dict["desk_code"]
    trade_date = report_dict["trade_date"]

    # BOILERPLATE — ensure processed_at_et is ET-aware for timestamp formatting
    if processed_at_et.tzinfo is None:
        processed_at_et = _ET.localize(processed_at_et)

    timestamp_str = processed_at_et.strftime("%Y%m%dT%H%M%S")
    s3_key = f"reports/{desk_code}_{trade_date}_positions_report_{timestamp_str}.json"

    # LOGIC — serialise report dict to JSON bytes, sanitising NaN/Inf → None
    sanitised = _sanitise(report_dict)
    json_bytes = json.dumps(sanitised, indent=2).encode("utf-8")

    # BOILERPLATE — write to existing S3 bucket; never create the bucket
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_bytes,
        ContentType="application/json",
        ContentEncoding="utf-8",
    )

    logger.info("Report written to s3://%s/%s (%d bytes)", bucket, s3_key, len(json_bytes))
    return s3_key