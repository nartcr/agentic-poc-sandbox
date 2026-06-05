# BOILERPLATE
import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of required fields per BAC-2
REQUIRED_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_blank(value) -> bool:
    """Return True if value is None, NaN, empty string, or whitespace-only."""
    # LOGIC
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _check_missing_fields(row: pd.Series) -> str | None:
    """Return rejection reason if any required field is missing, else None."""
    # LOGIC
    for field in REQUIRED_FIELDS:
        if field not in row.index or _is_blank(row.get(field)):
            return f"Missing required field: {field}"
    return None


def _check_trade_date_format(row: pd.Series) -> str | None:
    """Return rejection reason if trade_date is not parseable as YYYY-MM-DD, else None."""
    # LOGIC
    trade_date_val = str(row["trade_date"]).strip()
    try:
        date.fromisoformat(trade_date_val)  # strict YYYY-MM-DD on Python 3.7+
    except ValueError:
        return "Invalid trade_date format: expected YYYY-MM-DD"
    return None


def _check_notional_numeric(row: pd.Series) -> str | None:
    """Return rejection reason if notional_amount is not numeric, else None."""
    # LOGIC
    try:
        float(str(row["notional_amount"]).strip())
    except (ValueError, TypeError):
        return "Invalid notional_amount: not numeric"
    return None


def _check_notional_positive(row: pd.Series) -> str | None:
    """Return rejection reason if notional_amount is <= 0, else None."""
    # LOGIC
    value = float(str(row["notional_amount"]).strip())
    if value <= 0:
        return "Invalid notional_amount: must be positive"
    return None


def _check_desk_code_consistency(row: pd.Series, filename_desk_code: str) -> str | None:
    """Return rejection reason if row desk_code does not match filename desk_code."""
    # LOGIC
    row_desk_code = str(row["desk_code"]).strip()
    if row_desk_code != filename_desk_code:
        return f"desk_code mismatch: file says {filename_desk_code}, row says {row_desk_code}"
    return None


def _check_trade_date_consistency(row: pd.Series, filename_trade_date: str) -> str | None:
    """Return rejection reason if row trade_date does not match filename trade_date."""
    # LOGIC
    row_trade_date = str(row["trade_date"]).strip()
    if row_trade_date != filename_trade_date:
        return f"trade_date mismatch: file says {filename_trade_date}, row says {row_trade_date}"
    return None


def validate_rows(
    df: pd.DataFrame,
    filename_desk_code: str,
    filename_trade_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply per-row validation rules and split into valid and rejected DataFrames.

    Returns (valid_df, rejected_df).
    rejected_df includes all original columns plus 'rejection_reason'.
    valid_df has columns cast to final types.
    """
    # BOILERPLATE
    if df.empty:
        empty_rejected = df.copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), empty_rejected

    # LOGIC — phase 1: row-level checks (rules 1–6) applied in order
    rejection_reasons: dict[int, str] = {}

    for idx, row in df.iterrows():
        reason = (
            _check_missing_fields(row)
            or _check_trade_date_format(row)
            or _check_notional_numeric(row)
            or _check_notional_positive(row)
            or _check_desk_code_consistency(row, filename_desk_code)
            or _check_trade_date_consistency(row, filename_trade_date)
        )
        if reason:
            rejection_reasons[idx] = reason

    # LOGIC — phase 2: intra-file duplicate trade_id check (rule 7)
    # Only check rows that passed earlier rules
    candidate_indices = [i for i in df.index if i not in rejection_reasons]
    if candidate_indices:
        candidate_df = df.loc[candidate_indices]
        # Mark all occurrences after the first as duplicates
        duplicate_mask = candidate_df["trade_id"].duplicated(keep="first")
        for idx in candidate_df[duplicate_mask].index:
            rejection_reasons[idx] = "Duplicate trade_id within file"

    # LOGIC — build rejected_df
    rejected_indices = list(rejection_reasons.keys())
    valid_indices = [i for i in df.index if i not in rejection_reasons]

    if rejected_indices:
        rejected_df = df.loc[rejected_indices].copy()
        rejected_df["rejection_reason"] = [rejection_reasons[i] for i in rejected_indices]
    else:
        rejected_df = df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)

    # LOGIC — build valid_df with final cast types
    if valid_indices:
        valid_df = df.loc[valid_indices].copy()
        valid_df["trade_id"] = valid_df["trade_id"].astype(str)
        valid_df["desk_code"] = valid_df["desk_code"].astype(str)
        valid_df["trade_date"] = pd.to_datetime(valid_df["trade_date"]).dt.date
        valid_df["instrument_type"] = valid_df["instrument_type"].astype(str)
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)
        valid_df["currency"] = valid_df["currency"].astype(str)
        valid_df["counterparty_id"] = valid_df["counterparty_id"].astype(str)
    else:
        valid_df = pd.DataFrame(columns=REQUIRED_FIELDS)

    logger.info(
        "Validation complete: %d valid, %d rejected",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df