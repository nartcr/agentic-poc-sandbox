# BOILERPLATE
import io
import json
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — the seven mandatory columns as defined in the DATA CONTRACT
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET = pytz.timezone("America/Toronto")


# LOGIC
def _compute_null_rates(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    total_rows: int,
) -> "dict[str, float]":
    """
    Compute per-mandatory-column null rate = null_count / total_rows_received.
    Combines valid and rejected DataFrames so the denominator covers all input rows.
    """
    # LOGIC — combine all rows to count nulls across the full received set
    frames = [df for df in (valid_df, rejected_df) if df is not None and not df.empty]
    if not frames or total_rows == 0:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    combined = pd.concat(frames, ignore_index=True, sort=False)

    null_rates: dict = {}
    for col in _MANDATORY_COLUMNS:
        if col in combined.columns:
            # LOGIC — treat empty strings as null, consistent with row_validator
            null_count = combined[col].isnull().sum() + (
                combined[col]
                .astype(str)
                .str.strip()
                .eq("")
                .sum()
            )
            null_rates[col] = round(float(null_count) / float(total_rows), 6)
        else:
            null_rates[col] = 0.0

    return null_rates


# LOGIC
def _compute_notional_stats(
    valid_df: pd.DataFrame,
) -> "tuple[float | None, float | None]":
    """
    Return (min_notional_amount, max_notional_amount) from valid rows.
    Returns (None, None) when valid_df is empty or column is absent.
    """
    if valid_df is None or valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None

    # LOGIC — coerce to numeric; any non-parseable value becomes NaN and is ignored
    amounts = pd.to_numeric(valid_df["notional_amount"], errors="coerce").dropna()
    if amounts.empty:
        return None, None

    return float(amounts.min()), float(amounts.max())


# LOGIC
def _compute_rows_by_desk_code(valid_df: pd.DataFrame) -> "dict[str, int]":
    """
    Count valid rows grouped by desk_code column value.
    Returns an empty dict when valid_df is empty.
    """
    if valid_df is None or valid_df.empty or "desk_code" not in valid_df.columns:
        return {}

    grouped = (
        valid_df["desk_code"]
        .value_counts()
        .sort_index()  # LOGIC — deterministic ordering (TAC-7 / ORDER BY equivalent)
        .to_dict()
    )
    return {str(k): int(v) for k, v in grouped.items()}


# LOGIC
def write_summary_report(
    total_rows: int,
    rows_loaded: int,
    rejected_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    source_file_key: str,
    bucket: str,
) -> dict:
    """
    Compute summary statistics, write JSON report to S3, and return the report dict.

    S3 key: reports/{desk_code}_{trade_date}_positions_report.json
    """
    # LOGIC — resolve counts
    rows_rejected = len(rejected_df) if rejected_df is not None else 0

    # LOGIC — ET timestamp for regulatory conformance (TAC-7)
    processing_timestamp = datetime.now(_ET).isoformat()

    # LOGIC — per-column null rates across all received rows
    null_rates = _compute_null_rates(valid_df, rejected_df, total_rows)

    # LOGIC — notional stats from valid rows only
    min_notional, max_notional = _compute_notional_stats(valid_df)

    # LOGIC — valid rows grouped by desk_code
    rows_by_desk_code = _compute_rows_by_desk_code(valid_df)

    # LOGIC — S3 report key per DATA CONTRACT
    report_s3_key = f"reports/{desk_code}_{trade_date}_positions_report.json"

    # LOGIC — assemble the full report dict matching the SNS SUCCESS message schema
    report: dict = {
        "message_type": "SUCCESS",
        "total_rows_received": total_rows,
        "rows_successfully_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "processing_timestamp": processing_timestamp,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file_key": source_file_key,
        "rows_by_desk_code": rows_by_desk_code,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
        "report_s3_key": report_s3_key,
    }

    # BOILERPLATE — serialise to JSON bytes
    report_json_bytes = json.dumps(report, indent=2, default=str).encode("utf-8")

    # BOILERPLATE — write to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=report_s3_key,
        Body=report_json_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Summary report written: s3://%s/%s  total=%d loaded=%d rejected=%d",
        bucket,
        report_s3_key,
        total_rows,
        rows_loaded,
        rows_rejected,
    )

    return report