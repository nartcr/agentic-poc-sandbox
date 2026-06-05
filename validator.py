# BOILERPLATE
import logging
import math
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC
REQUIRED_FIELDS = [
    "trade_id",
    "desk_code",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "trade_date",
]


def _is_blank(value) -> bool:
    # LOGIC — treat None, NaN, and whitespace-only strings as blank
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return str(value).strip() == ""


def _check_required_fields(row: pd.Series) -> str | None:
    # LOGIC — Rule 1: all required fields present, non-null, non-empty
    for field in REQUIRED_FIELDS:
        if field not in row.index or _is_blank(row[field]):
            return f"missing_required_field: {field}"
    return None


def _check_trade_date(row: pd.Series, expected_trade_date: str) -> str | None:
    # LOGIC — Rule 2: trade_date parses as YYYY-MM-DD and matches filename date
    raw_value = str(row["trade_date"]).strip()
    try:
        datetime.strptime(raw_value, "%Y-%m-%d")
    except ValueError:
        return f"invalid_trade_date_format: '{raw_value}' is not YYYY-MM-DD"
    if raw_value != expected_trade_date:
        return (
            f"trade_date_mismatch: row has '{raw_value}'"
            f" but filename specifies '{expected_trade_date}'"
        )
    return None


def _check_desk_code(row: pd.Series, expected_desk_code: str) -> str | None:
    # LOGIC — Rule 3: desk_code matches the desk code parsed from the filename
    row_desk = str(row["desk_code"]).strip()
    if row_desk != expected_desk_code:
        return (
            f"desk_code_mismatch: row has '{row_desk}'"
            f" but filename specifies '{expected_desk_code}'"
        )
    return None


def _check_notional_amount(row: pd.Series) -> str | None:
    # LOGIC — Rule 4: notional_amount is a valid finite decimal
    raw_value = row["notional_amount"]
    try:
        value = float(raw_value)
    except (ValueError, TypeError):
        return f"invalid_notional_amount: '{raw_value}' is not a valid decimal"
    if not math.isfinite(value):
        return f"invalid_notional_amount: '{raw_value}' is not a finite number"
    return None


def _check_currency(row: pd.Series) -> str | None:
    # LOGIC — Rule 5: currency is exactly 3 uppercase alphabetic characters
    raw_value = str(row["currency"]).strip()
    if not re.fullmatch(r"[A-Z]{3}", raw_value):
        return (
            f"invalid_currency: '{raw_value}' is not a 3-character uppercase ISO 4217 code"
        )
    return None


def _check_trade_id_nonempty(row: pd.Series) -> str | None:
    # LOGIC — Rule 6: trade_id is non-empty string (catches empty strings that
    # survived the null check in rule 1 only if _is_blank uses strict NaN check)
    raw_value = str(row["trade_id"]).strip()
    if raw_value == "":
        return "empty_trade_id: trade_id must be a non-empty string"
    return None


# LOGIC — ordered list of (rule_function, *extra_args) applied per row
# Each callable accepts the row and any extra positional args; returns error str or None
_RULE_REGISTRY = [
    (_check_required_fields,),
    (_check_trade_date,),          # requires expected_trade_date
    (_check_desk_code,),           # requires expected_desk_code
    (_check_notional_amount,),
    (_check_currency,),
    (_check_trade_id_nonempty,),
]


def validate_rows(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate each row of df against the six ordered business rules.

    Returns (valid_df, rejected_df).
    rejected_df has all original columns plus 'rejection_reason'.
    First failing rule wins per row.
    """
    # BOILERPLATE
    logger.info(
        "Starting validation: %d rows, desk_code=%s, trade_date=%s",
        len(df),
        desk_code,
        trade_date,
    )

    # LOGIC — collect indices of valid and rejected rows
    valid_indices: list[int] = []
    rejected_records: list[dict] = []

    for idx, row in df.iterrows():
        rejection_reason: str | None = None

        # Rule 1 — required fields
        rejection_reason = _check_required_fields(row)

        # Rule 2 — trade_date format and match (only if rule 1 passed)
        if rejection_reason is None:
            rejection_reason = _check_trade_date(row, trade_date)

        # Rule 3 — desk_code match (only if rule 2 passed)
        if rejection_reason is None:
            rejection_reason = _check_desk_code(row, desk_code)

        # Rule 4 — notional_amount valid finite decimal (only if rule 3 passed)
        if rejection_reason is None:
            rejection_reason = _check_notional_amount(row)

        # Rule 5 — currency ISO 4217 (only if rule 4 passed)
        if rejection_reason is None:
            rejection_reason = _check_currency(row)

        # Rule 6 — trade_id non-empty (only if rule 5 passed)
        if rejection_reason is None:
            rejection_reason = _check_trade_id_nonempty(row)

        if rejection_reason is None:
            valid_indices.append(idx)
        else:
            record = row.to_dict()
            record["rejection_reason"] = rejection_reason
            rejected_records.append(record)

    # LOGIC — build output DataFrames
    valid_df = df.loc[valid_indices].reset_index(drop=True) if valid_indices else df.iloc[0:0].copy()

    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
        # LOGIC — ensure column order: original columns first, then rejection_reason
        original_cols = list(df.columns)
        rejected_df = rejected_df[original_cols + ["rejection_reason"]]
    else:
        rejected_df = df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype="str")

    logger.info(
        "Validation complete: %d valid, %d rejected",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df