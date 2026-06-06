# BOILERPLATE
import logging
import re
from datetime import datetime

import pandas as pd

# BOILERPLATE — import mandatory field list from config to avoid duplication
from src.config import MANDATORY_FIELDS

logger = logging.getLogger(__name__)

# LOGIC — compiled regex for ISO 4217 currency shape: exactly 3 uppercase alpha characters
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# LOGIC — strict date format for trade_date validation
_DATE_FORMAT = "%Y-%m-%d"


def _is_blank(value) -> bool:
    # LOGIC — treat NaN, None, and whitespace-only strings as blank
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _check_row(row: pd.Series, desk_code_from_filename: str, trade_date_from_filename: str):
    # LOGIC — apply V-01 through V-07 in order; return the rejection_reason string
    # for the first failing rule, or None if the row is fully valid.

    # V-01: all mandatory fields must be present and non-empty
    for field in MANDATORY_FIELDS:
        if field not in row.index or _is_blank(row.get(field)):
            return f"MISSING_FIELD:{field}"

    # V-02: trade_id must be a non-empty string with no whitespace-only content
    trade_id = row["trade_id"]
    if not isinstance(trade_id, str) or trade_id.strip() == "":
        return "INVALID_TRADE_ID"

    # V-03: trade_date must be parseable as YYYY-MM-DD and match the filename date
    trade_date_val = row["trade_date"]
    if not isinstance(trade_date_val, str):
        trade_date_val = str(trade_date_val)
    trade_date_val = trade_date_val.strip()
    try:
        datetime.strptime(trade_date_val, _DATE_FORMAT)
    except ValueError:
        return "INVALID_TRADE_DATE"
    if trade_date_val != trade_date_from_filename:
        return "INVALID_TRADE_DATE"

    # V-04: desk_code in the row must match the desk_code parsed from the filename
    desk_code_val = row["desk_code"]
    if not isinstance(desk_code_val, str) or desk_code_val.strip() != desk_code_from_filename:
        return "DESK_CODE_MISMATCH"

    # V-05: notional_amount must be parseable as float and non-negative
    notional_raw = row["notional_amount"]
    try:
        notional_float = float(notional_raw)
    except (ValueError, TypeError):
        return "INVALID_NOTIONAL_AMOUNT"
    if pd.isna(notional_float) or notional_float < 0:
        return "INVALID_NOTIONAL_AMOUNT"

    # V-06: currency must be exactly 3 uppercase alphabetic characters (ISO 4217 shape)
    currency_val = row["currency"]
    if not isinstance(currency_val, str) or not _CURRENCY_RE.match(currency_val.strip()):
        return "INVALID_CURRENCY"

    # V-07: counterparty_id must be a non-empty string
    cp_val = row["counterparty_id"]
    if _is_blank(cp_val):
        return "MISSING_COUNTERPARTY_ID"

    return None


def validate_rows(
    df: pd.DataFrame,
    desk_code_from_filename: str,
    trade_date_from_filename: str,
) -> tuple:
    # LOGIC — apply _check_row to every row; split into valid and rejected DataFrames

    if df.empty:
        logger.warning("validate_rows received an empty DataFrame")
        valid_df = df.copy()
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        return valid_df, rejected_df

    # LOGIC — compute rejection reason for every row; None means the row is valid
    rejection_reasons = df.apply(
        lambda row: _check_row(row, desk_code_from_filename, trade_date_from_filename),
        axis=1,
    )

    # LOGIC — split by whether a rejection reason was produced
    rejected_mask = rejection_reasons.notna()
    valid_mask = ~rejected_mask

    valid_df = df[valid_mask].copy()
    # LOGIC — valid_df must not carry a rejection_reason column
    if "rejection_reason" in valid_df.columns:
        valid_df = valid_df.drop(columns=["rejection_reason"])

    rejected_df = df[rejected_mask].copy()
    rejected_df["rejection_reason"] = rejection_reasons[rejected_mask]

    logger.info(
        "validate_rows: total=%d valid=%d rejected=%d",
        len(df),
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df