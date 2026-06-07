# BOILERPLATE
import json
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — mandatory columns used for null-rate computation
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def build_report(
    filename: str,
    desk_code: str,
    trade_date: str,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    processing_timestamp_et: datetime,
) -> dict:
    # LOGIC — compute derived counts per TAC-4
    total_rows_received = len(valid_df) + len(rejected_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    logger.info(
        "Building report: filename=%s total=%d valid=%d rejected=%d inserted=%d skipped=%d",
        filename,
        total_rows_received,
        len(valid_df),
        rows_rejected,
        rows_inserted,
        rows_skipped_duplicate,
    )

    # LOGIC — desk_breakdown: group valid_df by desk_code column
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_breakdown = (
            valid_df.groupby("desk_code", sort=True)
            .size()
            .to_dict()
        )
        # Convert numpy int64 values to plain Python int for JSON serialization
        desk_breakdown = {k: int(v) for k, v in desk_breakdown.items()}
    else:
        desk_breakdown = {}

    # LOGIC — notional min/max: only computed when valid rows exist
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min = float(notional_series.min()) if not notional_series.isna().all() else None
        notional_max = float(notional_series.max()) if not notional_series.isna().all() else None
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null_rates: computed across all rows (valid + rejected combined) before split
    # Reconstruct combined frame by dropping rejection_reason from rejected_df
    null_rates = _compute_null_rates(valid_df, rejected_df, total_rows_received)

    # LOGIC — processing_timestamp_et as ISO-8601 string with ET offset
    processing_ts_str = processing_timestamp_et.isoformat()

    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_ts_str,
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_breakdown": desk_breakdown,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info("Report built successfully for filename=%s", filename)
    return report


def _compute_null_rates(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    total_rows: int,
) -> dict:
    # LOGIC — combine valid and rejected frames on the 7 mandatory columns only
    # to compute null rates across the full input population
    if total_rows == 0:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    frames = []

    if not valid_df.empty:
        cols_present = [c for c in _MANDATORY_COLUMNS if c in valid_df.columns]
        frames.append(valid_df[cols_present].copy())

    if not rejected_df.empty:
        # Drop rejection_reason if present before combining
        rejected_core = rejected_df.drop(
            columns=["rejection_reason"], errors="ignore"
        )
        cols_present = [c for c in _MANDATORY_COLUMNS if c in rejected_core.columns]
        frames.append(rejected_core[cols_present].copy())

    if not frames:
        return {col: 0.0 for col in _MANDATORY_COLUMNS}

    combined = pd.concat(frames, ignore_index=True, sort=False)

    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col not in combined.columns:
            # LOGIC — column entirely absent means 100% null
            null_rates[col] = 1.0
        else:
            # LOGIC — treat empty strings as null (consistent with validator)
            col_series = combined[col].replace("", pd.NA)
            null_count = col_series.isna().sum()
            null_rates[col] = round(float(null_count) / total_rows, 6)

    return null_rates