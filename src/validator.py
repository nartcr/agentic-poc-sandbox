# BOILERPLATE
import datetime
import logging

import pandas as pd

# LOGIC — import mandatory field list from centralised config
from src.config import MANDATORY_FIELDS

logger = logging.getLogger(__name__)

# LOGIC — date format that trade_date values must conform to
_TRADE_DATE_FORMAT = "%Y-%m-%d"


def _is_blank(value: str) -> bool:
    """Returns True if the value is None, empty, or whitespace-only."""
    # LOGIC
    if value is None:
        return True
    return str(value).strip() == ""


def _try_cast_float(value: str) -> bool:
    """Returns True if value can be cast to float after stripping whitespace."""
    # LOGIC
    try:
        float(str(value).strip())
        return True
    except (ValueError, TypeError):
        return False


def _try_cast_date(value: str) -> bool:
    """Returns True if value matches YYYY-MM-DD format exactly."""
    # LOGIC
    try:
        datetime.datetime.strptime(str(value).strip(), _TRADE_DATE_FORMAT)
        return True
    except (ValueError, TypeError):
        return False


def validate(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> tuple:
    """
    Validates each row of the raw DataFrame against field-level rules.

    Checks applied in order per row (first failure wins):
      1. Missing mandatory fields (null / empty / whitespace-only)
      2. trade_date column value does not match YYYY-MM-DD
      3. notional_amount cannot be cast to float
      4. Row's desk_code does not match filename-extracted desk_code

    Returns:
        (valid_df, rejected_df)
        - valid_df:    original columns + row_number (int), notional_amount cast
                       to float, trade_date cast to datetime.date
        - rejected_df: original columns + row_number (int) + rejection_reason (str)
    """
    # BOILERPLATE — work on a copy; do not mutate caller's DataFrame
    working = df.copy()

    # LOGIC — assign 1-based row numbers reflecting source file position
    working["row_number"] = range(1, len(working) + 1)

    valid_rows = []
    rejected_rows = []

    for _, row in working.iterrows():
        row_dict = row.to_dict()
        rejection_reason = _check_row(row_dict, desk_code)

        if rejection_reason is not None:
            row_dict["rejection_reason"] = rejection_reason
            rejected_rows.append(row_dict)
        else:
            valid_rows.append(row_dict)

    # LOGIC — build rejected DataFrame
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows)
        # Ensure rejection_reason column is present and typed as str
        rejected_df["rejection_reason"] = rejected_df["rejection_reason"].astype(str)
    else:
        # LOGIC — empty rejected DataFrame preserves column schema
        rejected_columns = list(working.columns) + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=rejected_columns)

    # LOGIC — build valid DataFrame with coerced types
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)
        valid_df = _coerce_valid_types(valid_df)
    else:
        # LOGIC — empty valid DataFrame preserves column schema
        valid_df = pd.DataFrame(columns=list(working.columns))

    logger.info(
        "Validation complete — valid=%d, rejected=%d",
        len(valid_df),
        len(rejected_df),
    )
    if len(rejected_df) > 0:
        logger.warning(
            "%d row(s) rejected; first reason: %s",
            len(rejected_df),
            rejected_df["rejection_reason"].iloc[0],
        )

    return valid_df, rejected_df


def _check_row(row_dict: dict, filename_desk_code: str) -> str | None:
    """
    Applies validation checks in order.
    Returns the rejection_reason string, or None if the row is valid.
    """
    # LOGIC — Check 1: missing mandatory fields (first failing field wins)
    for field in MANDATORY_FIELDS:
        value = row_dict.get(field)
        if _is_blank(value):
            return f"MISSING_FIELD:{field}"

    # LOGIC — Check 2: trade_date format must be YYYY-MM-DD
    trade_date_value = row_dict.get("trade_date", "")
    if not _try_cast_date(trade_date_value):
        return "INVALID_DATE_FORMAT:trade_date"

    # LOGIC — Check 3: notional_amount must be castable to float
    notional_value = row_dict.get("notional_amount", "")
    if not _try_cast_float(notional_value):
        return "INVALID_NUMERIC:notional_amount"

    # LOGIC — Check 4: desk_code in row must match filename-extracted desk_code
    row_desk_code = str(row_dict.get("desk_code", "")).strip()
    if row_desk_code != filename_desk_code:
        return "DESK_CODE_MISMATCH"

    return None


def _coerce_valid_types(valid_df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerces columns on the valid DataFrame:
      - notional_amount -> float
      - trade_date      -> datetime.date
    All other columns remain as-is.
    """
    # LOGIC — cast notional_amount to float
    valid_df = valid_df.copy()
    valid_df["notional_amount"] = valid_df["notional_amount"].str.strip().astype(float)

    # LOGIC — cast trade_date string to datetime.date objects
    valid_df["trade_date"] = pd.to_datetime(
        valid_df["trade_date"].str.strip(), format=_TRADE_DATE_FORMAT
    ).dt.date

    return valid_df