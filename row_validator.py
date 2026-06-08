# BOILERPLATE
import re
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory fields per data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_blank(value) -> bool:
    # LOGIC — treat None, NaN, empty string, and whitespace-only as blank
    if value is None:
        return True
    if isinstance(value, float):
        # catches numpy NaN coming through as float
        import math
        if math.isnan(value):
            return True
    str_value = str(value).strip()
    return str_value == "" or str_value.lower() == "nan"


def _check_presence(row: pd.Series) -> str | None:
    # LOGIC — presence check: all 7 mandatory fields must be non-null, non-empty, non-whitespace
    for field in _MANDATORY_FIELDS:
        if field not in row.index or _is_blank(row[field]):
            return f"missing mandatory field: {field}"
    return None


def _check_trade_date(value: str) -> str | None:
    # LOGIC — trade_date must parse as YYYY-MM-DD
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except (ValueError, TypeError):
        return "invalid trade_date format: expected YYYY-MM-DD"
    return None


def _check_notional_amount(value: str) -> str | None:
    # LOGIC — notional_amount must be convertible to Decimal
    try:
        Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return f"non-numeric notional_amount: {value}"
    return None


def _check_currency(value: str) -> str | None:
    # LOGIC — currency must be exactly 3 alphabetic characters
    if not re.fullmatch(r"[A-Za-z]{3}", str(value).strip()):
        return "invalid currency format: must be 3 alpha characters"
    return None


def _validate_row(row: pd.Series) -> str | None:
    # LOGIC — apply rules in priority order; return first failing reason or None
    reason = _check_presence(row)
    if reason is not None:
        return reason

    reason = _check_trade_date(row["trade_date"])
    if reason is not None:
        return reason

    reason = _check_notional_amount(row["notional_amount"])
    if reason is not None:
        return reason

    reason = _check_currency(row["currency"])
    if reason is not None:
        return reason

    return None


def validate_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — validate each row; split into valid and rejected sets
    if df.empty:
        logger.info("Input DataFrame is empty; returning empty valid and rejected sets.")
        valid_df = df.copy()
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        return valid_df, rejected_df

    logger.info("Validating %d rows from input DataFrame.", len(df))

    rejection_reasons: list[str | None] = []

    for idx, row in df.iterrows():
        reason = _validate_row(row)
        rejection_reasons.append(reason)

    # LOGIC — build boolean mask: True where row is valid (no rejection reason)
    valid_mask = [reason is None for reason in rejection_reasons]
    invalid_mask = [reason is not None for reason in rejection_reasons]

    valid_df = df[valid_mask].copy().reset_index(drop=True)

    rejected_df = df[invalid_mask].copy().reset_index(drop=True)
    # LOGIC — attach rejection_reason column with the first failing rule description
    rejected_reasons_list = [r for r in rejection_reasons if r is not None]
    rejected_df["rejection_reason"] = rejected_reasons_list

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows.",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df