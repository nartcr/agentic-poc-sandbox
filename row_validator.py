# BOILERPLATE
import logging
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of required columns; used for presence checks before per-field validation
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — regex for currency: exactly 3 alphabetic characters
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")


def _is_missing(value) -> bool:
    # LOGIC — treat pandas NA, None, and blank strings as missing
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _is_valid_date(value: str) -> bool:
    # LOGIC — value must be parseable as YYYY-MM-DD; strict format, no partial matches
    if _is_missing(value):
        return False
    try:
        date.fromisoformat(str(value).strip())
        return True
    except (ValueError, AttributeError):
        return False


def _is_valid_decimal(value: str) -> bool:
    # LOGIC — value must be parseable as a finite decimal (rejects NaN, Inf strings)
    if _is_missing(value):
        return False
    try:
        d = Decimal(str(value).strip())
        # Reject special values: Infinity, -Infinity, NaN
        if not d.is_finite():
            return False
        return True
    except InvalidOperation:
        return False


def _is_valid_currency(value: str) -> bool:
    # LOGIC — exactly 3 alphabetic characters, case-insensitive
    if _is_missing(value):
        return False
    return bool(_CURRENCY_RE.match(str(value).strip()))


def _validate_row(row: pd.Series) -> list:
    # LOGIC — applies all seven validation rules in order; returns list of rejection reason strings
    reasons = []

    # Rule 1: trade_id non-null, non-empty
    if _is_missing(row.get("trade_id")):
        reasons.append("trade_id: missing or empty")

    # Rule 2: desk_code non-null, non-empty
    if _is_missing(row.get("desk_code")):
        reasons.append("desk_code: missing or empty")

    # Rule 3: trade_date parseable as YYYY-MM-DD
    if not _is_valid_date(row.get("trade_date")):
        reasons.append("trade_date: missing or not in YYYY-MM-DD format")

    # Rule 4: instrument_type non-null, non-empty
    if _is_missing(row.get("instrument_type")):
        reasons.append("instrument_type: missing or empty")

    # Rule 5: notional_amount parseable as finite decimal
    if not _is_valid_decimal(row.get("notional_amount")):
        reasons.append("notional_amount: missing or not a valid number")

    # Rule 6: currency exactly 3 alphabetic characters
    if not _is_valid_currency(row.get("currency")):
        reasons.append("currency: missing or not a 3-character alpha code")

    # Rule 7: counterparty_id non-null, non-empty
    if _is_missing(row.get("counterparty_id")):
        reasons.append("counterparty_id: missing or empty")

    return reasons


def _cast_valid_row(row: pd.Series) -> pd.Series:
    # LOGIC — type-casts trade_date to datetime.date and notional_amount to Decimal
    # Only called on rows that have already passed all validation rules
    row = row.copy()
    row["trade_date"] = date.fromisoformat(str(row["trade_date"]).strip())
    row["notional_amount"] = Decimal(str(row["notional_amount"]).strip())
    return row


def validate_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates every row of the raw DataFrame against mandatory-field and format rules.

    Returns:
        valid_df   — rows that passed all rules, with trade_date cast to datetime.date
                     and notional_amount cast to Decimal; no rejection_reason column.
        rejected_df — rows that failed one or more rules, with all original columns
                      plus a rejection_reason column (comma-separated reasons).
    """
    # BOILERPLATE
    if df.empty:
        logger.warning("validate_rows received an empty DataFrame; returning empty valid and rejected sets.")
        valid_df = df.copy()
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        return valid_df, rejected_df

    # LOGIC — ensure all required columns are present; missing columns treated as entirely null
    for col in _REQUIRED_COLUMNS:
        if col not in df.columns:
            logger.warning("Expected column '%s' not found in DataFrame; treating all values as missing.", col)
            df = df.copy()
            df[col] = None

    valid_indices = []
    rejected_rows = []

    # LOGIC — iterate every row, collect reasons; separate into valid and rejected buckets
    for idx, row in df.iterrows():
        reasons = _validate_row(row)
        if reasons:
            rejected_row = row.copy()
            rejected_row["rejection_reason"] = ", ".join(reasons)
            rejected_rows.append(rejected_row)
            logger.debug("Row %s rejected: %s", idx, rejected_row["rejection_reason"])
        else:
            valid_indices.append(idx)

    # LOGIC — build valid_df from passing rows, then apply type casts
    if valid_indices:
        valid_df = df.loc[valid_indices].copy()
        valid_df = valid_df.apply(_cast_valid_row, axis=1)
        logger.info("validate_rows: %d rows passed validation.", len(valid_df))
    else:
        # Return empty DataFrame with same columns (no rejection_reason)
        valid_df = df.iloc[0:0].copy()
        logger.info("validate_rows: 0 rows passed validation.")

    # LOGIC — build rejected_df from failed rows
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows, columns=list(df.columns) + ["rejection_reason"])
        logger.info("validate_rows: %d rows failed validation.", len(rejected_df))
    else:
        rejected_df = pd.DataFrame(columns=list(df.columns) + ["rejection_reason"])
        logger.info("validate_rows: 0 rows failed validation.")

    return valid_df, rejected_df