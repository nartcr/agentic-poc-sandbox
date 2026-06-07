# BOILERPLATE
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns used in null/empty checks
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_missing(value) -> bool:
    # LOGIC — returns True if the value is pandas NA or an empty/whitespace-only string
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _is_valid_date(value) -> bool:
    # LOGIC — returns True if value is a non-null string parseable as YYYY-MM-DD
    if _is_missing(value):
        return False
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _is_numeric(value) -> bool:
    # LOGIC — returns True if value can be cast to float (and is not missing)
    if _is_missing(value):
        return False
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def _validate_row(row: pd.Series, desk_code: str, trade_date: str):
    # LOGIC — applies all nine validation rules in order.
    # Returns None if the row is valid, or a rejection_reason string on first failure.

    # Rule 1: trade_id present
    if _is_missing(row.get("trade_id")):
        return "trade_id: missing or empty"

    # Rule 2: desk_code present in row
    if _is_missing(row.get("desk_code")):
        return "desk_code: missing or empty"

    # Rule 3: trade_date present and valid YYYY-MM-DD
    if not _is_valid_date(row.get("trade_date")):
        return "trade_date: missing or invalid format"

    # Rule 4: instrument_type present
    if _is_missing(row.get("instrument_type")):
        return "instrument_type: missing or empty"

    # Rule 5: notional_amount present and numeric
    if not _is_numeric(row.get("notional_amount")):
        return "notional_amount: missing or non-numeric"

    # Rule 6: currency present
    if _is_missing(row.get("currency")):
        return "currency: missing or empty"

    # Rule 7: counterparty_id present
    if _is_missing(row.get("counterparty_id")):
        return "counterparty_id: missing or empty"

    # Rule 8: trade_date in row must match trade_date from filename
    row_trade_date = str(row.get("trade_date", "")).strip()
    if row_trade_date != trade_date:
        return f"trade_date: does not match filename trade_date {trade_date}"

    # Rule 9: desk_code in row must match desk_code from filename
    row_desk_code = str(row.get("desk_code", "")).strip()
    if row_desk_code != desk_code:
        return f"desk_code: does not match filename desk_code {desk_code}"

    return None


def validate_rows(df: pd.DataFrame, desk_code: str, trade_date: str):
    # LOGIC — iterates all rows, applies _validate_row, splits into valid and rejected DataFrames.

    if df.empty:
        logger.warning("validate_rows received an empty DataFrame.")
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), rejected_df

    rejection_reasons = []

    for _, row in df.iterrows():
        reason = _validate_row(row, desk_code, trade_date)
        rejection_reasons.append(reason)

    # LOGIC — build boolean mask: True where row is valid (no rejection reason)
    valid_mask = pd.Series([r is None for r in rejection_reasons], index=df.index)
    rejected_mask = ~valid_mask

    valid_df = df[valid_mask].copy().reset_index(drop=True)

    rejected_df = df[rejected_mask].copy().reset_index(drop=True)
    # LOGIC — append rejection_reason as the last column on rejected rows
    rejected_reasons_series = pd.Series(
        [r for r in rejection_reasons if r is not None],
        index=rejected_df.index,
        dtype=str,
    )
    rejected_df["rejection_reason"] = rejected_reasons_series

    logger.info(
        "Validation complete: %d valid row(s), %d rejected row(s) (desk_code='%s', trade_date='%s')",
        len(valid_df),
        len(rejected_df),
        desk_code,
        trade_date,
    )

    if not rejected_df.empty:
        reason_counts = rejected_df["rejection_reason"].value_counts().to_dict()
        logger.info("Rejection reason breakdown: %s", reason_counts)

    return valid_df, rejected_df