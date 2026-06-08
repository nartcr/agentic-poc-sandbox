# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Tuple

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — the seven mandatory field names as specified in the data contracts
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — trade_date must be exactly YYYY-MM-DD
_TRADE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# LOGIC — currency must be exactly 3 uppercase ASCII letters
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _is_blank(value) -> bool:
    # LOGIC — treat pandas NA/NaN and empty/whitespace-only strings as blank
    if pd.isna(value):
        return True
    return str(value).strip() == ""


def _validate_row(row: pd.Series, desk_code: str, trade_date: str) -> str | None:
    """
    # LOGIC
    Apply all seven validation checks in the documented order.
    Returns the rejection_reason string if the row fails any check,
    or None if the row is valid.
    Stops at the first failing check (fail-fast per row).
    """

    # --- Check 1: Mandatory field presence ---
    # LOGIC — all seven fields must be non-null and non-empty
    for field in MANDATORY_FIELDS:
        val = row.get(field)
        if _is_blank(val):
            return f"Missing mandatory field: {field}"

    # --- Check 2: trade_id format ---
    # LOGIC — non-empty string, max 100 characters
    trade_id_val = str(row["trade_id"]).strip()
    if len(trade_id_val) == 0:
        return "trade_id is empty"
    if len(trade_id_val) > 100:
        return f"trade_id exceeds 100 characters: '{trade_id_val[:20]}...'"

    # --- Check 3: desk_code consistency ---
    # LOGIC — must match the desk_code parsed from the filename
    row_desk_code = str(row["desk_code"]).strip()
    if row_desk_code != desk_code:
        return f"desk_code mismatch: expected '{desk_code}', got '{row_desk_code}'"

    # --- Check 4: trade_date format and filename match ---
    # LOGIC — must be valid YYYY-MM-DD and match trade_date from filename
    row_trade_date = str(row["trade_date"]).strip()
    if not _TRADE_DATE_RE.match(row_trade_date):
        return f"trade_date format invalid: '{row_trade_date}'"
    # LOGIC — confirm it is an actually parseable calendar date
    try:
        datetime.strptime(row_trade_date, "%Y-%m-%d")
    except ValueError:
        return f"trade_date format invalid: '{row_trade_date}'"
    # LOGIC — must match the date from the filename
    if row_trade_date != trade_date:
        return f"trade_date mismatch: expected '{trade_date}', got '{row_trade_date}'"

    # --- Check 5: notional_amount numeric (NUMERIC(20,4) compatible) ---
    # LOGIC — must be parseable as Decimal without error
    notional_val = str(row["notional_amount"]).strip()
    try:
        Decimal(notional_val)
    except InvalidOperation:
        return f"notional_amount is not numeric: '{notional_val}'"

    # --- Check 6: currency format ---
    # LOGIC — exactly 3 uppercase alphabetic characters (ISO 4217)
    currency_val = str(row["currency"]).strip()
    if not _CURRENCY_RE.match(currency_val):
        return f"currency must be 3 uppercase letters: '{currency_val}'"

    # --- Check 7: counterparty_id ---
    # LOGIC — non-empty, max 100 characters
    counterparty_val = str(row["counterparty_id"]).strip()
    if len(counterparty_val) == 0:
        return "counterparty_id is empty"
    if len(counterparty_val) > 100:
        return f"counterparty_id exceeds 100 characters: '{counterparty_val[:20]}...'"

    # LOGIC — all checks passed
    return None


def validate_rows(
    df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    # LOGIC
    Validate every row in df against the seven ordered checks.
    Returns (valid_df, rejected_df).
    rejected_df contains all original columns plus 'rejection_reason'.
    valid_df contains only rows that passed all checks (no extra column).
    """
    logger.info(
        "Starting row validation: total_rows=%d, desk_code=%s, trade_date=%s",
        len(df),
        desk_code,
        trade_date,
    )

    valid_indices = []
    rejected_records = []

    # LOGIC — iterate rows; apply checks; split into valid/rejected sets
    for idx, row in df.iterrows():
        reason = _validate_row(row, desk_code, trade_date)
        if reason is None:
            valid_indices.append(idx)
        else:
            # LOGIC — preserve all original column values plus rejection_reason
            rejected_record = row.to_dict()
            rejected_record["rejection_reason"] = reason
            rejected_records.append(rejected_record)

    # LOGIC — build valid DataFrame from original rows (preserves dtypes)
    if valid_indices:
        valid_df = df.loc[valid_indices].reset_index(drop=True)
    else:
        valid_df = df.iloc[0:0].copy().reset_index(drop=True)

    # LOGIC — build rejected DataFrame with rejection_reason column appended
    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
        # LOGIC — ensure column order: original columns first, then rejection_reason
        original_cols = list(df.columns)
        col_order = original_cols + ["rejection_reason"]
        # LOGIC — only include columns that actually exist (guard against unexpected input)
        col_order = [c for c in col_order if c in rejected_df.columns]
        rejected_df = rejected_df[col_order].reset_index(drop=True)
    else:
        # LOGIC — empty rejected DataFrame with correct columns
        empty_cols = list(df.columns) + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=empty_cols)

    logger.info(
        "Row validation complete: valid=%d, rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df