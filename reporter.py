import io
import json
import logging
import math
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

from exceptions import FileReadError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — mandatory columns tracked for null_rates
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — required columns for the rejection CSV (ordered per data contract)
_REJECTION_COLUMNS = [
    "_row_number",
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def _et_now_iso() -> str:
    # BOILERPLATE — current ET timestamp as ISO-8601 string with UTC offset
    et = pytz.timezone("America/Toronto")
    return datetime.now(et).isoformat()


def _safe_float(value) -> float | None:
    # LOGIC — convert a pandas scalar to float, returning None for NaN/inf
    if value is None:
        return None
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    inserted_count: int,
    desk_code: str,
    trade_date: str,
    source_file: str,
) -> dict:
    # LOGIC — compute null rates for each mandatory column present in raw_df
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col in raw_df.columns:
            null_rates[col] = round(float(raw_df[col].isna().mean()), 6)
        else:
            null_rates[col] = 1.0  # column entirely absent — 100% null rate

    # LOGIC — compute counts_by_desk from raw_df groupby
    if "desk_code" in raw_df.columns:
        counts_by_desk = raw_df.groupby("desk_code").size().to_dict()
        # Convert numpy int to native int for JSON serialization
        counts_by_desk = {k: int(v) for k, v in counts_by_desk.items()}
    else:
        counts_by_desk = {}

    # LOGIC — min/max notional only when valid rows exist
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        min_notional = _safe_float(valid_df["notional_amount"].min())
        max_notional = _safe_float(valid_df["notional_amount"].max())
    else:
        min_notional = None
        max_notional = None

    report = {
        "total_rows": len(raw_df),
        "rows_loaded": inserted_count,
        "rows_rejected": len(rejected_df),
        "processing_timestamp": _et_now_iso(),
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file": source_file,
        "counts_by_desk": counts_by_desk,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built: total_rows=%d rows_loaded=%d rows_rejected=%d desk_code=%s trade_date=%s",
        report["total_rows"],
        report["rows_loaded"],
        report["rows_rejected"],
        desk_code,
        trade_date,
    )
    return report


def write_report(report_dict: dict, desk_code: str, trade_date: str) -> None:
    # LOGIC — serialize report_dict to JSON and write to S3
    bucket = os.environ["S3_REPORT_BUCKET"]
    key = f"reports/{trade_date}/{desk_code}_summary.json"

    def _json_default(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    report_json = json.dumps(report_dict, indent=2, default=_json_default)
    report_bytes = report_json.encode("utf-8")

    try:
        client = boto3.client("s3")
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=report_bytes,
            ContentType="application/json",
        )
        logger.info("Report written to s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error(
            "Failed to write report to s3://%s/%s: %s", bucket, key, str(exc),
            exc_info=True,
        )
        raise


def write_error_file(
    rejected_df: pd.DataFrame, desk_code: str, trade_date: str
) -> None:
    # LOGIC — write rejection CSV to S3 with exactly the required columns in order
    bucket = os.environ["S3_REPORT_BUCKET"]
    key = f"errors/{trade_date}/{desk_code}_rejections.csv"

    # LOGIC — select only the required columns in the specified order;
    # ensure any missing columns are added as empty strings to avoid KeyError
    output_df = rejected_df.copy()
    for col in _REJECTION_COLUMNS:
        if col not in output_df.columns:
            output_df[col] = ""
    output_df = output_df[_REJECTION_COLUMNS]

    # LOGIC — write DataFrame to in-memory CSV buffer (no /tmp/ path)
    csv_buffer = io.StringIO()
    output_df.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    try:
        client = boto3.client("s3")
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_bytes,
            ContentType="text/csv",
        )
        logger.info(
            "Error file written to s3://%s/%s (%d rejected rows)",
            bucket,
            key,
            len(output_df),
        )
    except Exception as exc:
        logger.error(
            "Failed to write error file to s3://%s/%s: %s", bucket, key, str(exc),
            exc_info=True,
        )
        raise