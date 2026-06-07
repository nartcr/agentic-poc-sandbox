# BOILERPLATE
import logging
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — mandatory columns for null-rate computation (matches data contract)
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _current_et_iso() -> str:
    # LOGIC — always ET, never UTC (TAC-7)
    et_tz = pytz.timezone("America/Toronto")
    now_et = datetime.now(et_tz)
    return now_et.isoformat()


def _compute_notional_stats(valid_df: pd.DataFrame) -> dict | None:
    # LOGIC — returns {min, max} as floats or None when no valid rows
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        logger.debug("No valid rows — notional_stats set to null")
        return None

    try:
        amounts = valid_df["notional_amount"].apply(
            lambda v: float(v) if not pd.isnull(v) else None
        ).dropna()

        if amounts.empty:
            return None

        return {
            "min": float(amounts.min()),
            "max": float(amounts.max()),
        }
    except (TypeError, ValueError, InvalidOperation) as exc:
        # LOGIC — log and return None rather than crashing the pipeline
        logger.warning("Could not compute notional_stats: %s", exc)
        return None


def _compute_null_rates(valid_df: pd.DataFrame, rejected_df: pd.DataFrame) -> dict:
    # LOGIC — null rates computed over the full population (valid + rejected)
    # A value is "null/empty" if it is NaN, None, or an empty/whitespace-only string
    frames = []
    if not valid_df.empty:
        frames.append(valid_df)
    if not rejected_df.empty:
        # rejected_df has an extra column rejection_reason — we only look at mandatory cols
        frames.append(rejected_df)

    if not frames:
        # LOGIC — no rows at all: every rate is 0.0 (can't divide by zero)
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    combined = pd.concat(frames, ignore_index=True, sort=False)
    total = len(combined)

    null_rates: dict = {}
    for col in _MANDATORY_COLUMNS:
        if col not in combined.columns:
            null_rates[col] = 1.0
            continue

        series = combined[col]
        # LOGIC — treat NaN, None, and blank strings as null
        null_mask = series.isna() | series.astype(str).str.strip().eq("")
        null_rates[col] = round(null_mask.sum() / total, 6) if total > 0 else 0.0

    return null_rates


def _build_rejection_reasons(rejected_df: pd.DataFrame) -> list:
    # LOGIC — produces list of {row_index, rejection_reason} matching the report JSON contract
    if rejected_df.empty:
        return []

    result = []
    for idx, row in rejected_df.iterrows():
        reason = str(row.get("rejection_reason", "")) if pd.notna(row.get("rejection_reason")) else ""
        result.append({
            "row_index": int(idx),
            "rejection_reason": reason,
        })
    return result


def build_report(
    total_rows: int,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
) -> dict:
    # LOGIC — main aggregation function; satisfies BAC-4, BAC-7, TAC-4, TAC-7
    logger.info(
        "Building report: desk_code=%s trade_date=%s total_rows=%d "
        "valid=%d rejected=%d rows_inserted=%d",
        desk_code,
        trade_date,
        total_rows,
        len(valid_df),
        len(rejected_df),
        rows_inserted,
    )

    processing_timestamp_et = _current_et_iso()

    rows_rejected = len(rejected_df)
    # LOGIC — skipped = rows that passed validation but were already in the DB
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    notional_stats = _compute_notional_stats(valid_df)
    null_rates = _compute_null_rates(valid_df, rejected_df)
    rejection_reasons = _build_rejection_reasons(rejected_df)

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "processing_timestamp_et": processing_timestamp_et,
        "notional_stats": notional_stats,
        "null_rates": null_rates,
        "rejection_reasons": rejection_reasons,
    }

    logger.info(
        "Report built: rows_loaded=%d rows_rejected=%d rows_skipped_duplicate=%d "
        "processing_timestamp_et=%s",
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
        processing_timestamp_et,
    )

    return report