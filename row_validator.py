# BOILERPLATE
import re
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — ordered list of mandatory fields per data contract
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "trade_date",
]

# LOGIC — trade_date must match YYYY-MM-DD and be a valid calendar date
_TRADE_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# LOGIC — currency must be exactly 3 alphabetic characters
_CURRENCY_PATTERN = re.compile(r"^[A-Za-z]{3}$")


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — check each mandatory field in order; return first violation
    for field in MANDATORY_FIELDS:
        value = row.get(field, None)
        if value is None or str(value).strip() == "":
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date_format(value: str) -> str | None:
    # LOGIC — validate YYYY-MM-DD pattern and calendar validity
    stripped = str(value).strip()
    if not _TRADE_DATE_PATTERN.match(stripped):
        return f"Invalid trade_date format: {value}"
    try:
        datetime.strptime(stripped, "%Y-%m-%d")
    except ValueError:
        return f"Invalid trade_date format: {value}"
    return None


def _check_notional_amount(value: str) -> str | None:
    # LOGIC — must be a parseable positive decimal
    stripped = str(value).strip()
    try:
        amount = Decimal(stripped)
    except InvalidOperation:
        return f"Invalid notional_amount: {value}"
    if amount <= Decimal("0"):
        return f"Invalid notional_amount: {value}"
    return None


def _check_currency_format(value: str) -> str | None:
    # LOGIC — must be exactly 3 alphabetic characters (ISO 4217 shape)
    stripped = str(value).strip()
    if not _CURRENCY_PATTERN.fullmatch(stripped):
        return f"Invalid currency format: {value}"
    return None


def _check_desk_code_consistency(row: pd.Series, expected_desk_code: str) -> str | None:
    # LOGIC — row desk_code must match the desk_code parsed from the filename
    row_desk_code = str(row.get("desk_code", "")).strip()
    if row_desk_code != expected_desk_code:
        return f"desk_code mismatch: expected {expected_desk_code}, got {row_desk_code}"
    return None


def validate_rows(
    df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — validate every row in df; split into valid and rejected DataFrames
    """
    Validates each row in the raw DataFrame against mandatory-field and type rules.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from file_reader with all columns as strings.
    desk_code : str
        The desk code parsed from the filename — used for consistency check.
    trade_date : str
        The trade date parsed from the filename (informational; row-level date is validated independently).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (valid_df, rejected_df) — rejected_df includes all original columns plus 'rejection_reason'.
    """
    logger.info(
        "Starting row validation: total_rows=%d desk_code=%s trade_date=%s",
        len(df),
        desk_code,
        trade_date,
    )

    valid_rows = []
    rejected_rows = []

    for idx, row in df.iterrows():
        rejection_reason = _apply_checks(row, desk_code)

        if rejection_reason is None:
            # LOGIC — row passed all checks
            valid_rows.append(row.to_dict())
        else:
            # LOGIC — row failed at least one check; record first failing reason
            rejected_row = row.to_dict()
            rejected_row["rejection_reason"] = rejection_reason
            rejected_rows.append(rejected_row)

    # LOGIC — reconstruct DataFrames; preserve original column order for valid rows
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
    else:
        valid_df = pd.DataFrame(columns=df.columns.tolist())

    if rejected_rows:
        rejected_columns = df.columns.tolist() + ["rejection_reason"]
        rejected_df = pd.DataFrame(rejected_rows, columns=rejected_columns)
    else:
        rejected_columns = df.columns.tolist() + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=rejected_columns)

    logger.info(
        "Row validation complete: valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df


def _apply_checks(row: pd.Series, desk_code: str) -> str | None:
    # LOGIC — apply all five checks in order; return first failing reason or None
    reason = _check_mandatory_fields(row)
    if reason is not None:
        return reason

    reason = _check_trade_date_format(row.get("trade_date", ""))
    if reason is not None:
        return reason

    reason = _check_notional_amount(row.get("notional_amount", ""))
    if reason is not None:
        return reason

    reason = _check_currency_format(row.get("currency", ""))
    if reason is not None:
        return reason

    reason = _check_desk_code_consistency(row, desk_code)
    if reason is not None:
        return reason

    return None