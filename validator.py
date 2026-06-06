import logging
import re
from datetime import datetime

import pandas as pd

from exceptions import ValidationError  # BOILERPLATE

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — canonical list of mandatory columns in definition order
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "trade_date",
]

# LOGIC — compiled regex for YYYY-MM-DD date format check
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_empty(value) -> bool:
    # LOGIC — treat pandas NA/NaN and blank strings as missing
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _cast_notional(value: str):
    # LOGIC — attempt float cast; return None on failure
    try:
        result = float(value)
        return result
    except (TypeError, ValueError):
        return None


def _validate_row(row: pd.Series, desk_code: str, trade_date: str) -> str | None:
    """
    Apply validation checks in priority order.
    Returns a rejection reason string if the row fails any check, or None if valid.
    """
    # LOGIC — Check 1: missing mandatory fields (first failing field determines reason)
    for field in MANDATORY_FIELDS:
        field_value = row.get(field)
        if _is_empty(field_value):
            return f"Missing mandatory field: {field}"

    # LOGIC — Check 2: desk_code mismatch against filename-extracted value
    row_desk_code = str(row["desk_code"]).strip()
    if row_desk_code != desk_code:
        return (
            f"desk_code mismatch: file declares {desk_code}, "
            f"row contains {row_desk_code}"
        )

    # LOGIC — Check 3: trade_date mismatch against filename-extracted value
    row_trade_date = str(row["trade_date"]).strip()
    if row_trade_date != trade_date:
        return (
            f"trade_date mismatch: file declares {trade_date}, "
            f"row contains {row_trade_date}"
        )

    # LOGIC — Check 4: notional_amount must be castable to float and non-negative
    notional_raw = str(row["notional_amount"]).strip()
    notional_val = _cast_notional(notional_raw)
    if notional_val is None or notional_val < 0:
        return f"Invalid notional_amount: {notional_raw}"

    # LOGIC — Check 5: trade_date format must be YYYY-MM-DD
    # (row_trade_date already matches the file date string, but we still enforce format)
    if not _DATE_RE.match(row_trade_date):
        return f"Invalid trade_date format: {row_trade_date}"

    # LOGIC — additionally validate that the date is a real calendar date
    try:
        datetime.strptime(row_trade_date, "%Y-%m-%d")
    except ValueError:
        return f"Invalid trade_date format: {row_trade_date}"

    return None  # row passes all checks


def validate(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate each row of raw DataFrame.

    Returns:
        (valid_df, rejected_df)
        valid_df  — passing rows with notional_amount as float64 and
                    trade_date as datetime.date; includes _row_number column.
        rejected_df — failing rows with rejection_reason column and
                      columns: [_row_number, trade_id, desk_code, trade_date,
                                instrument_type, notional_amount, currency,
                                counterparty_id, rejection_reason].
    """
    # LOGIC — verify that all mandatory columns are present in the file at all
    missing_cols = [c for c in MANDATORY_FIELDS if c not in df.columns]
    if missing_cols:
        raise ValidationError(
            f"Input DataFrame is missing required columns: {missing_cols}"
        )

    # LOGIC — assign 1-based row numbers matching original file line numbers
    working_df = df.copy()
    working_df["_row_number"] = range(1, len(working_df) + 1)

    rejection_reasons: list[str | None] = []

    # LOGIC — evaluate each row; collect rejection reasons (None = valid)
    for _, row in working_df.iterrows():
        reason = _validate_row(row, desk_code, trade_date)
        rejection_reasons.append(reason)

    working_df["_rejection_reason"] = rejection_reasons

    # LOGIC — split into valid and rejected subsets
    valid_mask = working_df["_rejection_reason"].isna()
    rejected_mask = ~valid_mask

    valid_df = working_df[valid_mask].drop(columns=["_rejection_reason"]).copy()
    rejected_rows = working_df[rejected_mask].copy()

    # LOGIC — cast notional_amount to float64 on valid rows
    valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)

    # LOGIC — cast trade_date to datetime.date on valid rows
    valid_df["trade_date"] = pd.to_datetime(
        valid_df["trade_date"], format="%Y-%m-%d"
    ).dt.date

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_rows),
    )

    # LOGIC — build rejected_df with exactly the specified columns
    rejected_output_cols = [
        "_row_number",
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
        "rejection_reason",
    ]

    if len(rejected_rows) > 0:
        rejected_df = rejected_rows.rename(
            columns={"_rejection_reason": "rejection_reason"}
        )[rejected_output_cols].copy()
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        # LOGIC — return empty DataFrame with correct schema when no rejections
        rejected_df = pd.DataFrame(columns=rejected_output_cols)

    valid_df = valid_df.reset_index(drop=True)

    return valid_df, rejected_df