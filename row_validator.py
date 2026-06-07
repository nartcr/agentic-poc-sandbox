# BOILERPLATE
import re
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# BOILERPLATE — compiled once at module level for efficiency
_CURRENCY_RE = re.compile(r'^[A-Z]{3}$')

# LOGIC — ordered list of mandatory field names per data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_missing(value) -> bool:
    # LOGIC — treat None/NaN and empty string as missing
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _check_missing_mandatory(row: pd.Series):
    # LOGIC — Rule 1: all mandatory fields must be non-null and non-empty
    for field in _MANDATORY_FIELDS:
        if _is_missing(row.get(field)):
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date_format(row: pd.Series):
    # LOGIC — Rule 2: trade_date must parse as YYYY-MM-DD
    value = row.get("trade_date")
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return None
    except (ValueError, AttributeError):
        return f"Invalid trade_date format: {value}"


def _check_notional_amount(row: pd.Series):
    # LOGIC — Rule 3: notional_amount must be castable to NUMERIC (Decimal)
    value = row.get("notional_amount")
    try:
        Decimal(str(value).strip())
        return None
    except (InvalidOperation, ValueError, AttributeError):
        return f"Non-numeric notional_amount: {value}"


def _check_currency_format(row: pd.Series):
    # LOGIC — Rule 4: currency must be exactly 3 uppercase alpha characters
    value = row.get("currency")
    if not isinstance(value, str) or not _CURRENCY_RE.match(value.strip()):
        return f"Invalid currency format: {value}"
    return None


def validate_rows(df: pd.DataFrame) -> tuple:
    # LOGIC — applies all five validation rules in order; returns (valid_df, rejected_df)
    """
    Validates each row of df against the five ordered rules defined in the
    data contract. Returns (valid_df, rejected_df).  rejected_df carries an
    extra 'rejection_reason' column populated with the first failing rule
    description for each row.
    """
    if df.empty:
        logger.info("validate_rows received an empty DataFrame; returning empty splits.")
        empty_rejected = df.copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), empty_rejected

    # LOGIC — work on a copy; we will accumulate indices of rejected rows
    working = df.copy().reset_index(drop=True)
    rejection_reasons: dict = {}  # index -> reason string

    # --- Rules 1–4: applied row by row ---
    for idx, row in working.iterrows():
        reason = _check_missing_mandatory(row)
        if reason:
            rejection_reasons[idx] = reason
            continue

        reason = _check_trade_date_format(row)
        if reason:
            rejection_reasons[idx] = reason
            continue

        reason = _check_notional_amount(row)
        if reason:
            rejection_reasons[idx] = reason
            continue

        reason = _check_currency_format(row)
        if reason:
            rejection_reasons[idx] = reason
            continue

    # LOGIC — Rule 5: duplicate composite key (trade_id + desk_code + trade_date)
    #         Only applied to rows that passed rules 1–4
    passed_rules_1_to_4_idx = [i for i in working.index if i not in rejection_reasons]

    if passed_rules_1_to_4_idx:
        candidates = working.loc[passed_rules_1_to_4_idx, ["trade_id", "desk_code", "trade_date"]].copy()
        # LOGIC — mark every occurrence of a duplicated composite key except the first
        dup_mask = candidates.duplicated(subset=["trade_id", "desk_code", "trade_date"], keep="first")
        dup_indices = candidates.index[dup_mask].tolist()
        for idx in dup_indices:
            rejection_reasons[idx] = "Duplicate trade_id within file for desk_code/trade_date"

    # LOGIC — split into valid and rejected DataFrames
    rejected_indices = list(rejection_reasons.keys())
    valid_indices = [i for i in working.index if i not in rejection_reasons]

    valid_df = working.loc[valid_indices].copy().reset_index(drop=True)

    rejected_df = working.loc[rejected_indices].copy()
    rejected_df["rejection_reason"] = rejected_df.index.map(rejection_reasons)
    rejected_df = rejected_df.reset_index(drop=True)

    logger.info(
        "validate_rows complete: total=%d valid=%d rejected=%d",
        len(working),
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df