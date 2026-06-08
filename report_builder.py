# BOILERPLATE
import io
import json
import logging
import math
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET_TZ = pytz.timezone("America/Toronto")


# BOILERPLATE — custom encoder to handle numpy scalar types and non-finite floats
class _SafeJSONEncoder(json.JSONEncoder):
    """Handles numpy float types and non-finite float values (NaN, Inf) safely."""

    def default(self, obj):  # noqa: D401
        # BOILERPLATE
        try:
            import numpy as np  # only imported if numpy is available
            if isinstance(obj, (np.floating,)):
                if math.isfinite(float(obj)):
                    return float(obj)
                return None
            if isinstance(obj, (np.integer,)):
                return int(obj)
        except ImportError:
            pass
        return super().default(obj)

    def encode(self, obj):
        # BOILERPLATE — intercept top-level encode to sanitize Python floats too
        return super().encode(self._sanitize(obj))

    def iterencode(self, obj, _one_shot=False):
        # BOILERPLATE
        return super().iterencode(self._sanitize(obj), _one_shot=_one_shot)

    @staticmethod
    def _sanitize(obj):
        # LOGIC — recursively replace non-finite floats with None for JSON safety
        if isinstance(obj, float):
            if not math.isfinite(obj):
                return None
            return obj
        if isinstance(obj, dict):
            return {k: _SafeJSONEncoder._sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_SafeJSONEncoder._sanitize(v) for v in obj]
        return obj


def _current_et_timestamp() -> datetime:
    # LOGIC — single authoritative source of current ET time within this module
    return datetime.now(_ET_TZ)


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — null rate = (empty-string + NaN count) / total_rows, per column
    if raw_df.empty:
        return {}
    total = len(raw_df)
    null_rates = {}
    for col in raw_df.columns:
        series = raw_df[col]
        # Count NaN values
        nan_count = int(series.isna().sum())
        # Count empty-string values (columns are read as str; guard for non-str)
        try:
            empty_str_count = int((series == "").sum())
        except TypeError:
            empty_str_count = 0
        rate = (nan_count + empty_str_count) / total
        null_rates[col] = round(rate, 4)
    return null_rates


def _compute_notional_stats(valid_df: pd.DataFrame):
    # LOGIC — return (min, max) as Python floats, or (None, None) if valid_df is empty
    if valid_df.empty:
        return None, None
    try:
        notional_series = valid_df["notional_amount"].astype(float)
        min_val = float(notional_series.min())
        max_val = float(notional_series.max())
        # Guard against NaN resulting from an all-null column
        if not math.isfinite(min_val):
            min_val = None
        if not math.isfinite(max_val):
            max_val = None
        return min_val, max_val
    except (KeyError, ValueError, TypeError) as exc:
        logger.warning("Could not compute notional stats: %s", exc)
        return None, None


def _compute_counts_by_desk_code(valid_df: pd.DataFrame) -> dict:
    # LOGIC — group by desk_code and return counts as a plain Python dict
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    return {str(k): int(v) for k, v in valid_df.groupby("desk_code").size().items()}


def build_and_store_report(
    bucket: str,
    source_key: str,
    desk_code: str,
    trade_date: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
) -> str:
    """Build the processing summary report, write it to S3, and write the manifest.

    Returns the S3 key of the report file.
    """
    # LOGIC — capture ET timestamp once; reuse for report key and JSON field
    processing_ts = _current_et_timestamp()
    processing_ts_iso = processing_ts.isoformat()
    # Timestamp string for S3 key: yyyymmddTHHMMSS  (no colon, no timezone suffix)
    ts_for_key = processing_ts.strftime("%Y%m%dT%H%M%S")

    # LOGIC — derive the filename from the source_key (last path segment)
    filename = source_key.split("/")[-1]

    # LOGIC — compute all report metrics
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)
    min_notional, max_notional = _compute_notional_stats(valid_df)

    report_payload = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_ts_iso,
        "counts_by_desk_code": _compute_counts_by_desk_code(valid_df),
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates_per_column": _compute_null_rates(raw_df),
    }

    report_json_bytes = json.dumps(report_payload, cls=_SafeJSONEncoder, indent=2).encode("utf-8")

    # LOGIC — S3 key: reports/{desk_code}_{trade_date}_{yyyymmddTHHMMSS}_report.json
    report_s3_key = f"reports/{desk_code}_{trade_date}_{ts_for_key}_report.json"

    s3_client = boto3.client("s3")  # BOILERPLATE

    # LOGIC — write report JSON to S3
    logger.info("Writing report to s3://%s/%s", bucket, report_s3_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=report_s3_key,
        Body=report_json_bytes,
        ContentType="application/json",
    )

    # LOGIC — build manifest pointing to this report (predictable key, no timestamp)
    manifest_s3_key = f"manifests/{desk_code}_{trade_date}_manifest.json"
    manifest_payload = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_s3_key": report_s3_key,
        "generated_at_et": processing_ts_iso,
        # LOGIC — include files dict per manifest pattern rule for downstream consumers
        "files": {
            "report": report_s3_key,
        },
    }
    manifest_json_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")

    logger.info("Writing manifest to s3://%s/%s", bucket, manifest_s3_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_s3_key,
        Body=manifest_json_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report build complete. desk_code=%s trade_date=%s total_rows=%d "
        "rows_loaded=%d rows_rejected=%d report_key=%s",
        desk_code,
        trade_date,
        total_rows,
        rows_inserted,
        rows_rejected,
        report_s3_key,
    )

    return report_s3_key