# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)


# LOGIC
def get_mandatory_columns() -> list:
    """Returns the list of mandatory column names in the defined order."""
    return [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]


# LOGIC
def _is_blank(value) -> bool:
    """Returns True if value is None, NaN, empty string, or whitespace-only."""
    if value is None:
        return True
    str_val = str(value).strip()
    return str_val == "" or str_val.lower() == "nan"


# LOGIC
def _validate_trade_date(value: str):
    """
    Attempts to parse value as YYYY-MM-DD.
    Returns datetime.date on success, raises ValueError on failure.
    """
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


# LOGIC
def _validate_notional(value: str) -> Decimal:
    """
    Attempts to parse value as a finite Decimal.
    Returns Decimal on success, raises InvalidOperation or ValueError on failure.
    """
    try:
        dec = Decimal(str(value).strip())
    except InvalidOperation:
        raise ValueError(f"Cannot parse notional_amount as decimal: {value!r}")
    if not dec.is_finite():
        raise ValueError(f"notional_amount is not finite: {value!r}")
    return dec


# LOGIC
def _validate_currency(value: str) -> str:
    """
    Validates that value is exactly 3 alphabetic characters.
    Returns the stripped value on success, raises ValueError on failure.
    """
    stripped = str(value).strip()
    if not (len(stripped) == 3 and stripped.isalpha()):
        raise ValueError(f"currency must be exactly 3 alphabetic characters: {value!r}")
    return stripped


# LOGIC
def validate_rows(raw_df: pd.DataFrame):
    """
    Validates each row in raw_df against mandatory-field rules.

    Rules applied in order per row (first failure wins):
      1. MISSING_FIELD   — any mandatory column is null/empty/whitespace
      2. INVALID_TRADE_DATE — trade_date not parseable as YYYY-MM-DD
      3. INVALID_NOTIONAL   — notional_amount not parseable as finite decimal
      4. INVALID_CURRENCY   — currency is not exactly 3 alphabetic characters

    Returns:
        valid_df   — rows passing all checks, with typed columns:
                       trade_date -> datetime.date
                       notional_amount -> Decimal
        rejected_df — original raw rows + 'rejection_reason' column
    """
    mandatory_cols = get_mandatory_columns()

    # LOGIC — check that all mandatory columns exist in the DataFrame
    missing_cols = [c for c in mandatory_cols if c not in raw_df.columns]
    if missing_cols:
        logger.warning(
            "Input DataFrame is missing mandatory columns: %s — all rows rejected with MISSING_FIELD",
            missing_cols,
        )
        rejected = raw_df.copy()
        rejected["rejection_reason"] = f"MISSING_FIELD: columns absent: {missing_cols}"
        empty_valid = pd.DataFrame(columns=mandatory_cols)
        return empty_valid, rejected

    if raw_df.empty:
        logger.info("Input DataFrame is empty; returning empty valid and rejected sets.")
        empty_valid = pd.DataFrame(columns=mandatory_cols)
        empty_rejected = pd.DataFrame(columns=mandatory_cols + ["rejection_reason"])
        return empty_valid, empty_rejected

    valid_records = []    # list of dicts with coerced types
    rejected_records = [] # list of dicts: original row values + rejection_reason

    for idx, row in raw_df.iterrows():
        raw_row_dict = row.to_dict()
        rejection_reason = None

        # LOGIC — Rule 1: MISSING_FIELD
        for col in mandatory_cols:
            if _is_blank(row[col]):
                rejection_reason = f"MISSING_FIELD: {col}"
                break

        if rejection_reason is None:
            # LOGIC — Rule 2: INVALID_TRADE_DATE
            try:
                parsed_date = _validate_trade_date(row["trade_date"])
            except (ValueError, TypeError) as exc:
                rejection_reason = f"INVALID_TRADE_DATE: {row['trade_date']!r}"
                logger.debug("Row %s rejected — INVALID_TRADE_DATE: %s", idx, exc)

        if rejection_reason is None:
            # LOGIC — Rule 3: INVALID_NOTIONAL
            try:
                parsed_notional = _validate_notional(row["notional_amount"])
            except (ValueError, InvalidOperation, TypeError) as exc:
                rejection_reason = f"INVALID_NOTIONAL: {row['notional_amount']!r}"
                logger.debug("Row %s rejected — INVALID_NOTIONAL: %s", idx, exc)

        if rejection_reason is None:
            # LOGIC — Rule 4: INVALID_CURRENCY
            try:
                parsed_currency = _validate_currency(row["currency"])
            except (ValueError, TypeError) as exc:
                rejection_reason = f"INVALID_CURRENCY: {row['currency']!r}"
                logger.debug("Row %s rejected — INVALID_CURRENCY: %s", idx, exc)

        if rejection_reason is not None:
            # LOGIC — row is rejected; record original values + reason
            rejected_row = dict(raw_row_dict)
            rejected_row["rejection_reason"] = rejection_reason
            rejected_records.append(rejected_row)
            logger.debug("Row %s rejected: %s", idx, rejection_reason)
        else:
            # LOGIC — row is valid; record with coerced types
            valid_row = dict(raw_row_dict)
            valid_row["trade_date"] = parsed_date
            valid_row["notional_amount"] = parsed_notional
            valid_row["currency"] = parsed_currency
            valid_records.append(valid_row)

    # LOGIC — build output DataFrames
    if valid_records:
        valid_df = pd.DataFrame(valid_records)
    else:
        valid_df = pd.DataFrame(columns=raw_df.columns.tolist())

    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
    else:
        rejected_df = pd.DataFrame(columns=raw_df.columns.tolist() + ["rejection_reason"])

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows.",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df