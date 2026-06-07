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

# LOGIC — the 7 mandatory columns for null-rate computation (matches data contract)
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    """Compute null/empty rates per mandatory column from the raw (string) DataFrame.

    A cell is considered null if it is NaN, None, or an empty string after stripping.
    Returns a dict mapping column name -> float (rate between 0.0 and 1.0).
    """
    # LOGIC
    total = len(raw_df)
    rates = {}
    for col in _MANDATORY_COLUMNS:
        if col not in raw_df.columns:
            # Column entirely absent — treat as 100 % null
            rates[col] = 1.0
            continue
        if total == 0:
            rates[col] = 0.0
            continue
        null_count = (
            raw_df[col]
            .apply(
                lambda v: v is None
                or (isinstance(v, float) and pd.isna(v))
                or (isinstance(v, str) and v.strip() == "")
            )
            .sum()
        )
        rates[col] = float(null_count) / float(total)
    return rates


def _rows_by_desk_code(valid_df: pd.DataFrame) -> dict:
    """Return a dict of desk_code -> row count, sorted by desk_code (TAC-4).

    Computed from valid_df only (successfully loaded rows).
    """
    # LOGIC
    if valid_df is None or valid_df.empty:
        return {}
    grouped = (
        valid_df.groupby("desk_code", sort=True)
        .size()
        .reset_index(name="count")
        .sort_values("desk_code")
    )
    return {row["desk_code"]: int(row["count"]) for _, row in grouped.iterrows()}


def _notional_stats(valid_df: pd.DataFrame) -> "tuple[str | None, str | None]":
    """Return (min, max) of notional_amount as strings, or (None, None) if empty.

    valid_df.notional_amount is already cast to Decimal by row_validator.
    """
    # LOGIC
    if valid_df is None or valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    amounts = valid_df["notional_amount"].dropna()
    if amounts.empty:
        return None, None
    # Decimal comparisons work correctly; convert to string for JSON serialisation
    min_val = min(amounts)
    max_val = max(amounts)
    return str(min_val), str(max_val)


def _decimal_default(obj):
    """JSON serialiser fallback for Decimal values."""
    # BOILERPLATE
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")


def build_and_write_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    filename: str,
    desk_code: str,
    trade_date_str: str,
    timestamp_et: datetime,
    bucket: str,
    error_file_key: "str | None",
) -> "tuple[str, dict]":
    """Construct the summary report dict, write it to S3, and return (s3_key, report_dict).

    Never re-queries the database — all statistics are derived from the DataFrames passed in.
    The timestamp_et argument must be an ET-aware datetime (America/Toronto).
    """
    # LOGIC — build timestamp strings (ISO-8601 with ET offset, e.g. 2026-06-15T19:32:00-04:00)
    processing_timestamp_iso = timestamp_et.isoformat()

    # LOGIC — core row counts
    total_rows_received = len(raw_df) if raw_df is not None else 0
    rows_rejected = len(rejected_df) if rejected_df is not None else 0

    # LOGIC — notional amount stats from valid_df (Decimal-typed column)
    notional_min, notional_max = _notional_stats(valid_df)

    # LOGIC — null rates from raw_df (string-typed, unmodified incoming values)
    null_rates = _compute_null_rates(raw_df if raw_df is not None else pd.DataFrame())

    # LOGIC — rows by desk code from valid_df (sorted by desk_code per TAC-4)
    rows_by_desk = _rows_by_desk_code(valid_df)

    # LOGIC — assemble the report dict using exact field names from the design
    report_dict = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "processing_timestamp_et": processing_timestamp_iso,
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_by_desk_code": rows_by_desk,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates_per_column": null_rates,
        "error_file_s3_key": error_file_key,
    }

    # LOGIC — build the S3 key using the exact pattern from the design
    timestamp_str = timestamp_et.strftime("%Y%m%dT%H%M%S")
    s3_key = (
        f"reports/{desk_code}_{trade_date_str}_positions_report_{timestamp_str}.json"
    )

    # LOGIC — serialise to JSON (Decimal values converted to strings via custom default)
    report_json = json.dumps(report_dict, default=_decimal_default, indent=2)
    report_bytes = report_json.encode("utf-8")

    # BOILERPLATE — write the JSON report to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
        ContentEncoding="utf-8",
    )

    logger.info(
        "Report written to s3://%s/%s (total=%d inserted=%d rejected=%d)",
        bucket,
        s3_key,
        total_rows_received,
        rows_inserted,
        rows_rejected,
    )

    return s3_key, report_dict