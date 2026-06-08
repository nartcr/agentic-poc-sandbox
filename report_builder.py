# BOILERPLATE
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns used for null-rate computation (matches data contract)
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(combined_df: pd.DataFrame, total_rows: int) -> dict:
    # LOGIC — per-column null rate across combined (valid + rejected) rows for 7 mandatory columns
    null_rates: dict = {}
    for col in _MANDATORY_COLUMNS:
        if total_rows == 0:
            null_rates[col] = 0.0
        elif col not in combined_df.columns:
            # LOGIC — column entirely absent counts as 100% null
            null_rates[col] = 1.0
        else:
            # LOGIC — dtype=str from file_reader: treat NaN, None, and whitespace-only as null
            is_null = combined_df[col].isnull() | (
                combined_df[col].astype(str).str.strip() == ""
            )
            null_rates[col] = float(is_null.sum()) / total_rows
    return null_rates


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — convert notional_amount strings to float for min/max; return (None, None) if empty
    if valid_df.empty:
        return None, None
    try:
        notional_series = valid_df["notional_amount"].astype(float)
        return float(notional_series.min()), float(notional_series.max())
    except (ValueError, TypeError) as exc:
        # LOGIC — if conversion fails for any reason, log and return None rather than crashing
        logger.error("Failed to compute notional statistics: %s", exc)
        return None, None


def _compute_row_counts_by_desk(valid_df: pd.DataFrame) -> dict:
    # LOGIC — group valid rows by desk_code and return counts as plain Python dict
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    return {
        str(k): int(v)
        for k, v in valid_df.groupby("desk_code").size().to_dict().items()
    }


def build_report(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    source_filename: str,
    desk_code: str,
    trade_date: str,
) -> dict:
    # LOGIC — compute total row counts
    total_rows: int = len(valid_df) + len(rejected_df)
    rows_rejected: int = len(rejected_df)

    logger.info(
        "Building report: source=%s desk=%s trade_date=%s total=%d valid=%d rejected=%d inserted=%d",
        source_filename,
        desk_code,
        trade_date,
        total_rows,
        len(valid_df),
        rows_rejected,
        rows_inserted,
    )

    # LOGIC — current processing timestamp in ET (America/Toronto), never UTC
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et: datetime = datetime.now(et_tz)
    processing_timestamp_et_str: str = processing_timestamp_et.strftime(
        "%Y-%m-%dT%H:%M:%S%z"
    )

    # LOGIC — build combined DataFrame for null-rate computation
    # Drop rejection_reason from rejected_df before combining so column sets match
    if not rejected_df.empty and "rejection_reason" in rejected_df.columns:
        rejected_for_nulls = rejected_df.drop(columns=["rejection_reason"])
    else:
        rejected_for_nulls = rejected_df.copy()

    # LOGIC — concatenate valid and rejected (original columns only) for null-rate analysis
    if not valid_df.empty and not rejected_for_nulls.empty:
        combined_df = pd.concat(
            [valid_df, rejected_for_nulls], ignore_index=True, sort=False
        )
    elif not valid_df.empty:
        combined_df = valid_df.copy()
    elif not rejected_for_nulls.empty:
        combined_df = rejected_for_nulls.copy()
    else:
        combined_df = pd.DataFrame(columns=_MANDATORY_COLUMNS)

    # LOGIC — compute derived statistics
    null_rates = _compute_null_rates(combined_df, total_rows)
    min_notional, max_notional = _compute_notional_stats(valid_df)
    row_counts_by_desk = _compute_row_counts_by_desk(valid_df)

    # LOGIC — assemble report dict matching the S3 Report JSON Schema in the data contract
    report: dict = {
        "filename": source_filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_received": total_rows,
        "rows_inserted": int(rows_inserted),
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et_str,
        "row_counts_by_desk": row_counts_by_desk,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built successfully: total_rows=%d rows_inserted=%d rows_rejected=%d",
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    return report