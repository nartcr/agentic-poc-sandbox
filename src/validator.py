# BOILERPLATE
import logging
import re
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# BOILERPLATE — constants for expected columns and rejection reasons
_EXPECTED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# LOGIC — rejection reason strings (must match design verbatim)
_REJ_TRADE_ID = "trade_id: missing or empty"
_REJ_DESK_CODE = "desk_code: missing or empty"
_REJ_TRADE_DATE = "trade_date: missing or invalid format (expected YYYY-MM-DD)"
_REJ_INSTRUMENT_TYPE = "instrument_type: missing or empty"
_REJ_NOTIONAL_AMOUNT = "notional_amount: missing, non-numeric, or not positive"
_REJ_CURRENCY = "currency: missing or invalid (expected 3-letter ISO code)"
_REJ_COUNTERPARTY_ID = "counterparty_id: missing or empty"


# LOGIC — individual field validators

def _check_nonempty(value: str) -> bool:
    """Return True if value is a non-empty, non-whitespace string."""
    return isinstance(value, str) and len(value.strip()) > 0


def _check_trade_date(value: str) -> bool:
    """Return True if value is parseable as YYYY-MM-DD."""
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.strptime(value.strip(), "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _check_notional_amount(value: str) -> bool:
    """Return True if value is castable to float and strictly > 0."""
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        amount = float(value.strip())
    except ValueError:
        return False
    return amount > 0.0


def _check_currency(value: str) -> bool:
    """Return True if value is exactly 3 uppercase alpha characters."""
    if not isinstance(value, str):
        return False
    return bool(_CURRENCY_RE.fullmatch(value.strip()))


# LOGIC — row-level validation: returns first rejection reason or None

def _get_rejection_reason(row: pd.Series) -> Optional[str]:
    """
    Apply all seven validation rules in order.
    Returns the first matching rejection reason string, or None if valid.
    """
    if not _check_nonempty(row.get("trade_id", "")):
        return _REJ_TRADE_ID

    if not _check_nonempty(row.get("desk_code", "")):
        return _REJ_DESK_CODE

    if not _check_trade_date(row.get("trade_date", "")):
        return _REJ_TRADE_DATE

    if not _check_nonempty(row.get("instrument_type", "")):
        return _REJ_INSTRUMENT_TYPE

    if not _check_notional_amount(row.get("notional_amount", "")):
        return _REJ_NOTIONAL_AMOUNT

    if not _check_currency(row.get("currency", "")):
        return _REJ_CURRENCY

    if not _check_nonempty(row.get("counterparty_id", "")):
        return _REJ_COUNTERPARTY_ID

    return None


# LOGIC — main public function

def validate_positions(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply field-level validation rules to each row of the raw DataFrame.

    Parameters
    ----------
    df : raw DataFrame from file_reader (all columns are str)

    Returns
    -------
    (valid_df, rejected_df)

    - valid_df   : rows that passed all checks, with notional_amount cast to
                   float and trade_date cast to datetime.date.
    - rejected_df: rows that failed, retaining all original columns plus an
                   added ``rejection_reason`` column (str).
    """
    # LOGIC — validate every row and record rejection reason (or None)
    rejection_reasons = df.apply(_get_rejection_reason, axis=1)

    # LOGIC — split into two DataFrames based on presence of a rejection reason
    rejected_mask = rejection_reasons.notna()
    valid_mask = ~rejected_mask

    valid_df = df[valid_mask].copy().reset_index(drop=True)
    rejected_df = df[rejected_mask].copy().reset_index(drop=True)

    # LOGIC — add rejection_reason column to rejected rows
    rejected_df["rejection_reason"] = (
        rejection_reasons[rejected_mask].values
    )

    # LOGIC — cast notional_amount to float on valid rows
    if not valid_df.empty:
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)

    # LOGIC — cast trade_date to datetime.date on valid rows
    if not valid_df.empty:
        valid_df["trade_date"] = pd.to_datetime(
            valid_df["trade_date"], format="%Y-%m-%d"
        ).dt.date

    logger.info(
        "Validation complete — valid=%d, rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df