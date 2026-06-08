# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Tuple

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — canonical column set required by the data contract
REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — YYYY-MM-DD pattern for trade_date validation
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_blank(value: str) -> bool:
    # LOGIC — treat None, NaN-coerced empty string, and whitespace-only as blank
    if value is None:
        return True
    return str(value).strip() == ""


def _validate_single_row(row: pd.Series) -> str:
    # LOGIC — apply validation rules in prescribed order; return first failure reason or empty string
    trade_id = str(row.get("trade_id", ""))
    desk_code = str(row.get("desk_code", ""))
    trade_date = str(row.get("trade_date", ""))
    instrument_type = str(row.get("instrument_type", ""))
    notional_amount = str(row.get("notional_amount", ""))
    currency = str(row.get("currency", ""))
    counterparty_id = str(row.get("counterparty_id", ""))

    # LOGIC — rule 1: trade_id blank
    if _is_blank(trade_id):
        return "trade_id: missing or blank"

    # LOGIC — rule 2: desk_code blank
    if _is_blank(desk_code):
        return "desk_code: missing or blank"

    # LOGIC — rule 3: trade_date blank
    if _is_blank(trade_date):
        return "trade_date: missing or blank"

    # LOGIC — rule 4: trade_date not parseable as YYYY-MM-DD
    if not _DATE_PATTERN.match(trade_date.strip()):
        return "trade_date: invalid format, expected YYYY-MM-DD"
    try:
        datetime.strptime(trade_date.strip(), "%Y-%m-%d")
    except ValueError:
        return "trade_date: invalid format, expected YYYY-MM-DD"

    # LOGIC — rule 5: instrument_type blank
    if _is_blank(instrument_type):
        return "instrument_type: missing or blank"

    # LOGIC — rule 6: notional_amount blank
    if _is_blank(notional_amount):
        return "notional_amount: missing or blank"

    # LOGIC — rule 7: notional_amount non-numeric
    try:
        Decimal(notional_amount.strip())
    except InvalidOperation:
        return "notional_amount: non-numeric value"

    # LOGIC — rule 8: currency blank
    if _is_blank(currency):
        return "currency: missing or blank"

    # LOGIC — rule 9: currency not exactly 3 characters
    if len(currency.strip()) != 3:
        return "currency: must be exactly 3 characters"

    # LOGIC — rule 10: counterparty_id blank
    if _is_blank(counterparty_id):
        return "counterparty_id: missing or blank"

    return ""


def validate_rows(
    df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — handle empty input DataFrame immediately
    if df.empty:
        logger.info("validate_rows: input DataFrame is empty; returning empty valid and rejected DataFrames")
        empty_valid = pd.DataFrame(columns=REQUIRED_COLUMNS)
        empty_rejected = pd.DataFrame(columns=REQUIRED_COLUMNS + ["rejection_reason"])
        return empty_valid, empty_rejected

    # LOGIC — ensure all required columns exist; missing columns filled with empty string
    # so the blank-check rules naturally reject all rows for that column
    working_df = df.copy()
    for col in REQUIRED_COLUMNS:
        if col not in working_df.columns:
            logger.warning(
                "validate_rows: mandatory column '%s' is absent from DataFrame; treating all rows as blank for this column",
                col,
            )
            working_df[col] = ""

    # LOGIC — replace NaN / None with empty string for consistent blank detection
    working_df[REQUIRED_COLUMNS] = working_df[REQUIRED_COLUMNS].fillna("").astype(str)

    valid_indices = []
    rejected_indices = []
    rejection_reasons = {}

    # LOGIC — iterate row-by-row; first failure wins per row
    for idx, row in working_df.iterrows():
        reason = _validate_single_row(row)
        if reason:
            rejected_indices.append(idx)
            rejection_reasons[idx] = reason
        else:
            valid_indices.append(idx)

    logger.info(
        "validate_rows: desk_code=%s trade_date=%s total=%d valid=%d rejected=%d",
        desk_code,
        trade_date,
        len(working_df),
        len(valid_indices),
        len(rejected_indices),
    )

    # LOGIC — build rejected DataFrame: original columns + rejection_reason
    if rejected_indices:
        rejected_df = working_df.loc[rejected_indices].copy()
        rejected_df["rejection_reason"] = [rejection_reasons[i] for i in rejected_indices]
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        rejected_df = pd.DataFrame(columns=list(working_df.columns) + ["rejection_reason"])

    # LOGIC — build valid DataFrame: cast types per data contract
    if valid_indices:
        valid_df = working_df.loc[valid_indices, REQUIRED_COLUMNS].copy()
        valid_df = valid_df.reset_index(drop=True)
        valid_df = _cast_valid_types(valid_df)
    else:
        valid_df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    return valid_df, rejected_df


def _cast_valid_types(df: pd.DataFrame) -> pd.DataFrame:
    # LOGIC — cast trade_date to datetime.date and notional_amount to Decimal
    # These casts are safe here because all rows have already passed format validation.
    df = df.copy()

    df["trade_date"] = df["trade_date"].apply(
        lambda v: datetime.strptime(v.strip(), "%Y-%m-%d").date()
    )

    df["notional_amount"] = df["notional_amount"].apply(
        lambda v: Decimal(v.strip())
    )

    # LOGIC — strip leading/trailing whitespace from string columns
    for col in ("trade_id", "desk_code", "instrument_type", "currency", "counterparty_id"):
        df[col] = df[col].str.strip()

    return df