# BOILERPLATE
import re
import logging
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns as defined in the data contracts
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — max digits and decimal places matching NUMERIC(20,4)
NOTIONAL_MAX_DIGITS = 20
NOTIONAL_MAX_DECIMAL_PLACES = 4

# LOGIC — currency must be exactly 3 uppercase alpha characters
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _is_blank(value) -> bool:
    # LOGIC — treat None, NaN, empty string, and whitespace-only as missing
    if value is None:
        return True
    if not isinstance(value, str):
        # handles pandas NaN / float nan that arrives before string coercion
        try:
            import math
            if math.isnan(value):
                return True
        except (TypeError, ValueError):
            pass
        return False
    return value.strip() == ""


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — rule 1: missing mandatory fields; first missing field wins
    for field in MANDATORY_FIELDS:
        val = row.get(field)
        if _is_blank(val):
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date(row: pd.Series) -> str | None:
    # LOGIC — rule 2: trade_date must parse as YYYY-MM-DD
    value = str(row["trade_date"]).strip()
    try:
        pd.to_datetime(value, format="%Y-%m-%d")
    except (ValueError, TypeError):
        return f"Invalid trade_date format: {value}"
    return None


def _check_notional_amount(row: pd.Series) -> str | None:
    # LOGIC — rule 3: notional_amount must be parseable as Decimal with <=20 digits and <=4 decimal places
    value = str(row["notional_amount"]).strip()
    try:
        d = Decimal(value)
    except InvalidOperation:
        return f"Invalid notional_amount: {value}"

    sign, digits, exponent = d.as_tuple()

    # total significant digits must not exceed NOTIONAL_MAX_DIGITS
    if len(digits) > NOTIONAL_MAX_DIGITS:
        return f"Invalid notional_amount: {value}"

    # decimal places: negative exponent means decimal places
    if isinstance(exponent, int) and exponent < 0:
        decimal_places = -exponent
        if decimal_places > NOTIONAL_MAX_DECIMAL_PLACES:
            return f"Invalid notional_amount: {value}"

    return None


def _check_currency(row: pd.Series) -> str | None:
    # LOGIC — rule 4: currency must be exactly 3 uppercase alpha characters
    value = str(row["currency"]).strip()
    if not _CURRENCY_RE.fullmatch(value):
        return f"Invalid currency format: {value}"
    return None


# LOGIC — ordered validation rules; first failure wins per row
_VALIDATION_RULES = [
    _check_mandatory_fields,
    _check_trade_date,
    _check_notional_amount,
    _check_currency,
]


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply validation rules in sequence to each row of df.
    Returns (valid_df, rejected_df).
    rejected_df contains all original columns plus 'rejection_reason'.
    First failing rule per row wins; subsequent rules are not evaluated.
    """
    # LOGIC
    valid_indices = []
    rejected_rows = []  # list of (index, row_dict, rejection_reason)

    for idx, row in df.iterrows():
        rejection_reason = None
        for rule in _VALIDATION_RULES:
            reason = rule(row)
            if reason is not None:
                rejection_reason = reason
                break

        if rejection_reason is None:
            valid_indices.append(idx)
        else:
            row_dict = row.to_dict()
            row_dict["rejection_reason"] = rejection_reason
            rejected_rows.append(row_dict)

    # LOGIC — build valid DataFrame preserving original dtypes
    if valid_indices:
        valid_df = df.loc[valid_indices].reset_index(drop=True)
    else:
        valid_df = df.iloc[0:0].reset_index(drop=True)

    # LOGIC — build rejected DataFrame with rejection_reason column appended
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows, columns=list(df.columns) + ["rejection_reason"])
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        rejected_cols = list(df.columns) + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=rejected_cols)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df