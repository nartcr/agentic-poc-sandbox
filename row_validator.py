# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — mandatory columns that every row must contain
REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — ISO 4217 currency: exactly 3 ASCII alphabetic characters
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")

# LOGIC — trade_date must be YYYY-MM-DD
_DATE_FORMAT = "%Y-%m-%d"


def _check_presence(value) -> bool:
    # LOGIC — non-null, non-empty after strip
    if value is None:
        return False
    if pd.isna(value) if not isinstance(value, str) else False:
        return False
    return str(value).strip() != ""


def _validate_trade_date(raw_value: str) -> Tuple[bool, object]:
    # LOGIC — attempt YYYY-MM-DD parse; return (ok, date_obj)
    try:
        parsed = datetime.strptime(raw_value.strip(), _DATE_FORMAT).date()
        return True, parsed
    except (ValueError, AttributeError):
        return False, None


def _validate_notional(raw_value: str) -> Tuple[bool, object]:
    # LOGIC — must be a finite decimal number (no NaN, no Inf)
    try:
        d = Decimal(str(raw_value).strip())
        if not d.is_finite():
            return False, None
        return True, d
    except (InvalidOperation, ValueError):
        return False, None


def _validate_currency(raw_value: str) -> bool:
    # LOGIC — exactly 3 alphabetic characters
    return bool(_CURRENCY_RE.match(str(raw_value).strip()))


def _validate_row(
    row: pd.Series,
    filename_desk_code: str,
) -> Tuple[bool, str, dict]:
    """
    # LOGIC — validate a single row in order:
    1. Presence check for all required fields
    2. Type checks (trade_date, notional_amount, currency)
    3. Consistency check: desk_code matches filename
    Returns (is_valid, rejection_reason, typed_values_dict)
    """
    typed = {}

    # Step 1: Presence checks
    for col in REQUIRED_COLUMNS:
        raw = row.get(col, None)
        if not _check_presence(raw):
            return False, f"Field '{col}' is missing or empty.", {}

    # Step 2a: trade_date type check
    raw_trade_date = str(row["trade_date"]).strip()
    ok, parsed_date = _validate_trade_date(raw_trade_date)
    if not ok:
        return (
            False,
            f"Field 'trade_date' value '{raw_trade_date}' is not a valid date in YYYY-MM-DD format.",
            {},
        )
    typed["trade_date"] = parsed_date

    # Step 2b: notional_amount type check
    raw_notional = str(row["notional_amount"]).strip()
    ok, parsed_notional = _validate_notional(raw_notional)
    if not ok:
        return (
            False,
            f"Field 'notional_amount' value '{raw_notional}' is not a valid finite decimal number.",
            {},
        )
    typed["notional_amount"] = parsed_notional

    # Step 2c: currency type check
    raw_currency = str(row["currency"]).strip()
    if not _validate_currency(raw_currency):
        return (
            False,
            f"Field 'currency' value '{raw_currency}' is not a valid 3-character alphabetic ISO 4217 code.",
            {},
        )
    typed["currency"] = raw_currency.upper()

    # Step 3: Consistency check — desk_code in row must match filename
    row_desk_code = str(row["desk_code"]).strip()
    if row_desk_code != filename_desk_code.strip():
        return (
            False,
            f"Field 'desk_code' value '{row_desk_code}' does not match filename desk_code '{filename_desk_code}'.",
            {},
        )

    # Build typed values for the valid row
    typed["trade_id"] = str(row["trade_id"]).strip()
    typed["desk_code"] = row_desk_code
    typed["instrument_type"] = str(row["instrument_type"]).strip()
    typed["counterparty_id"] = str(row["counterparty_id"]).strip()

    return True, "", typed


def validate_rows(
    df: pd.DataFrame,
    filename_desk_code: str,
    filename_trade_date: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    # LOGIC — split raw DataFrame into valid and rejected sets.

    valid_df: typed columns (trade_date as date, notional_amount as Decimal)
    rejected_df: original string values plus rejection_reason column
    """
    # BOILERPLATE — guard: check required columns exist in the DataFrame
    missing_headers = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_headers:
        logger.error(
            "Input DataFrame is missing required columns: %s", missing_headers
        )
        raise ValueError(
            f"Input file is missing required columns: {missing_headers}"
        )

    valid_rows = []
    rejected_rows = []

    for idx, row in df.iterrows():
        is_valid, reason, typed_values = _validate_row(row, filename_desk_code)

        if is_valid:
            # LOGIC — build valid row with typed values in canonical column order
            valid_rows.append(
                {
                    "trade_id": typed_values["trade_id"],
                    "desk_code": typed_values["desk_code"],
                    "trade_date": typed_values["trade_date"],
                    "instrument_type": typed_values["instrument_type"],
                    "notional_amount": typed_values["notional_amount"],
                    "currency": typed_values["currency"],
                    "counterparty_id": typed_values["counterparty_id"],
                }
            )
        else:
            # LOGIC — preserve original raw values and append rejection reason
            rejected_row = {col: row.get(col, None) for col in REQUIRED_COLUMNS}
            rejected_row["rejection_reason"] = reason
            rejected_rows.append(rejected_row)

    # BOILERPLATE — construct output DataFrames
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=REQUIRED_COLUMNS)
    else:
        valid_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    if rejected_rows:
        rejected_df = pd.DataFrame(
            rejected_rows, columns=REQUIRED_COLUMNS + ["rejection_reason"]
        )
    else:
        rejected_df = pd.DataFrame(
            columns=REQUIRED_COLUMNS + ["rejection_reason"]
        )

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows.",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df