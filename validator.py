# BOILERPLATE
import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns used for null-rate checks and validation
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_null_or_empty(value) -> bool:
    # LOGIC — treats pandas NaN, None, and whitespace-only strings as missing
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _validate_trade_id(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "trade_id is missing"
    return None


def _validate_desk_code(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "desk_code is missing"
    return None


def _validate_trade_date(value) -> str | None:
    # LOGIC — checks not null, matches YYYY-MM-DD pattern, then verifiable calendar date
    if _is_null_or_empty(value):
        return "trade_date is missing"
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value).strip()):
        return "trade_date does not match format YYYY-MM-DD"
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except ValueError:
        return "trade_date is not a valid calendar date"
    return None


def _validate_instrument_type(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "instrument_type is missing"
    return None


def _validate_notional_amount(value) -> str | None:
    # LOGIC — must be castable to Decimal; rejects non-numeric characters
    if _is_null_or_empty(value):
        return "notional_amount is missing"
    try:
        Decimal(str(value).strip())
    except InvalidOperation:
        return "notional_amount is not a valid decimal"
    return None


def _validate_currency(value) -> str | None:
    # LOGIC — must be exactly 3 alphabetic characters (ISO 4217)
    if _is_null_or_empty(value):
        return "currency is missing"
    if not re.fullmatch(r"[A-Za-z]{3}", str(value).strip()):
        return "currency must be exactly 3 alphabetic characters"
    return None


def _validate_counterparty_id(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "counterparty_id is missing"
    return None


# LOGIC — maps each column to its validator function, in the order specified by the design
_VALIDATORS = [
    ("trade_id", _validate_trade_id),
    ("desk_code", _validate_desk_code),
    ("trade_date", _validate_trade_date),
    ("instrument_type", _validate_instrument_type),
    ("notional_amount", _validate_notional_amount),
    ("currency", _validate_currency),
    ("counterparty_id", _validate_counterparty_id),
]


def _validate_row(row: pd.Series) -> str | None:
    # LOGIC — applies all validators to a single row, accumulates all failure reasons
    reasons = []
    for col, validator_fn in _VALIDATORS:
        value = row.get(col)
        result = validator_fn(value)
        if result is not None:
            reasons.append(result)
    if reasons:
        return " | ".join(reasons)
    return None


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — entry point: applies row-level validation, splits into valid and rejected sets
    logger.info("Starting validation on %d rows", len(df))

    # LOGIC — apply _validate_row across all rows; result is a Series of reason strings or None
    rejection_reasons = df.apply(_validate_row, axis=1)

    # LOGIC — boolean mask: True where row is valid (no rejection reason)
    valid_mask = rejection_reasons.isna()

    valid_df = df.loc[valid_mask].copy().reset_index(drop=True)

    rejected_df = df.loc[~valid_mask].copy()
    rejected_df = rejected_df.reset_index(drop=True)
    # LOGIC — append rejection_reason as the final column on the rejected set
    rejected_df["rejection_reason"] = rejection_reasons.loc[~valid_mask].values

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df