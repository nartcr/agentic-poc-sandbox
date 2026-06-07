# BOILERPLATE
import logging
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz

import file_writer
from ingestion_exceptions import TradeIngestionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — mandatory column list from data contracts
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def build_and_store_report(
    bucket: str,
    filename: str,
    desk_code: str,
    trade_date: str,
    total_rows: int,
    rows_inserted: int,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
) -> tuple[dict, str]:
    """
    # LOGIC
    Compute post-load summary statistics, write the summary report JSON and manifest
    JSON to S3 via file_writer, and return (report_dict, report_s3_key).

    Parameters
    ----------
    bucket       : S3 bucket name
    filename     : original S3 key of the input file (e.g. incoming/EQTY_2026-06-01_positions.csv)
    desk_code    : parsed desk code string
    trade_date   : parsed trade date string (YYYY-MM-DD)
    total_rows   : total rows read from raw file (len of raw DataFrame before validation split)
    rows_inserted: count returned by position_loader.load_positions
    valid_df     : validated rows DataFrame (no rejection_reason column)
    rejected_df  : rejected rows DataFrame (includes rejection_reason column)
    """
    logger.info(
        "Building report for desk_code=%s trade_date=%s total_rows=%d rows_inserted=%d rows_rejected=%d",
        desk_code,
        trade_date,
        total_rows,
        rows_inserted,
        len(rejected_df),
    )

    processing_ts = _get_et_timestamp()

    # LOGIC — reconstruct full raw DataFrame for null_rates calculation
    # valid_df + rejected_df (without rejection_reason) = full raw population
    full_df = _reconstruct_full_df(valid_df, rejected_df, total_rows)
    null_rates = _compute_null_rates(full_df)

    # LOGIC — desk_code distribution from valid rows only
    desk_code_counts = _compute_desk_code_counts(valid_df)

    # LOGIC — notional statistics from valid rows only
    notional_min, notional_max = _compute_notional_stats(valid_df)

    # LOGIC — assemble the summary report dict per the data contract schema
    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": total_rows,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": len(rejected_df),
        "processing_timestamp_et": processing_ts,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    # LOGIC — write summary report JSON to S3
    report_key = file_writer.write_report_json(bucket, desk_code, trade_date, report)
    logger.info("Report written to s3://%s/%s", bucket, report_key)

    # LOGIC — derive the error file key using the same deterministic pattern as file_writer
    error_key = f"errors/{desk_code}_{trade_date}_rejected.csv"

    # LOGIC — write manifest JSON to S3 mapping logical names to actual keys
    manifest_key = file_writer.write_manifest(
        bucket,
        desk_code,
        trade_date,
        {"report": report_key, "errors": error_key},
    )
    logger.info("Manifest written to s3://%s/%s", bucket, manifest_key)

    return report, report_key


def _compute_null_rates(df: pd.DataFrame) -> dict[str, float]:
    """
    # LOGIC
    Compute the null (or empty-string) rate for each mandatory column over the full DataFrame.
    Rate = (number of null or empty-string values) / total_rows.
    Returns 0.0 for columns not present (should not happen in normal flow).
    """
    total = len(df)
    if total == 0:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    rates: dict[str, float] = {}
    for col in _MANDATORY_COLUMNS:
        if col not in df.columns:
            rates[col] = 0.0
            continue
        # LOGIC — treat NaN and empty string both as null since dtype=str read produces empty strings
        null_count = int(
            df[col]
            .apply(lambda v: v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "")
            .sum()
        )
        rates[col] = round(null_count / total, 6)

    return rates


def _get_et_timestamp() -> str:
    """
    # LOGIC
    Return the current wall-clock time as an ISO-8601 string in America/Toronto timezone.
    Example: '2026-06-01T19:45:00-04:00'
    """
    et_tz = pytz.timezone("America/Toronto")
    now_et = datetime.now(et_tz)
    return now_et.isoformat()


def _reconstruct_full_df(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    total_rows: int,
) -> pd.DataFrame:
    """
    # LOGIC
    Reconstruct the full raw population (valid + rejected without rejection_reason)
    for null_rates computation over the complete input.
    If both DataFrames are empty, return an empty DataFrame with mandatory columns.
    """
    rejected_base = rejected_df.drop(columns=["rejection_reason"], errors="ignore")

    frames = []
    if not valid_df.empty:
        frames.append(valid_df[_MANDATORY_COLUMNS] if set(_MANDATORY_COLUMNS).issubset(valid_df.columns) else valid_df)
    if not rejected_base.empty:
        frames.append(
            rejected_base[_MANDATORY_COLUMNS]
            if set(_MANDATORY_COLUMNS).issubset(rejected_base.columns)
            else rejected_base
        )

    if not frames:
        return pd.DataFrame(columns=_MANDATORY_COLUMNS)

    combined = pd.concat(frames, ignore_index=True)
    return combined


def _compute_desk_code_counts(valid_df: pd.DataFrame) -> dict[str, int]:
    """
    # LOGIC
    Compute a {desk_code: row_count} dict from the valid rows DataFrame.
    Returns an empty dict if valid_df is empty or desk_code column is absent.
    """
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    counts = valid_df["desk_code"].value_counts()
    return {str(k): int(v) for k, v in counts.items()}


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple[Optional[float], Optional[float]]:
    """
    # LOGIC
    Compute (notional_min, notional_max) as floats from the valid rows DataFrame.
    Returns (None, None) if valid_df is empty or has no notional_amount column,
    so the JSON report emits null for these fields when there are no valid rows.
    """
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None

    try:
        notional_series = valid_df["notional_amount"].astype(float)
        notional_min = float(notional_series.min())
        notional_max = float(notional_series.max())
        return notional_min, notional_max
    except (ValueError, TypeError) as exc:
        logger.warning("Could not compute notional stats: %s", exc)
        return None, None