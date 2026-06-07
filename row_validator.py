import logging
import math
import re
from datetime import date, datetime

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory fields per data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_missing(value) -> bool:
    # LOGIC — treat None, float NaN, and empty/whitespace-only strings as missing
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _validate_mandatory_fields(row: dict) -> str | None:
    # LOGIC — rule 1: all mandatory fields must be non-null and non-empty
    for field in _MANDATORY_FIELDS:
        if _is_missing(row.get(field)):
            return f"MISSING_FIELD:{field}"
    return None


def _validate_trade_id(row: dict) -> str | None:
    # LOGIC — rule 2: trade_id must have no leading/trailing whitespace
    trade_id = str(row["trade_id"])
    if trade_id != trade_id.strip():
        return "INVALID_FORMAT:trade_id"
    if trade_id.strip() == "":
        return "INVALID_FORMAT:trade_id"
    return None


def _validate_trade_date(row: dict, filename_trade_date: date) -> str | None:
    # LOGIC — rule 3: trade_date must be YYYY-MM-DD and match the filename date
    raw_val = str(row["trade_date"]).strip()
    try:
        parsed = datetime.strptime(raw_val, "%Y-%m-%d").date()
    except ValueError:
        return "INVALID_FORMAT:trade_date"
    if parsed != filename_trade_date:
        return "INVALID_FORMAT:trade_date"
    return None


def _validate_desk_code(row: dict, filename_desk_code: str) -> str | None:
    # LOGIC — rule 4: desk_code in row must match filename desk_code
    if str(row["desk_code"]).strip() != filename_desk_code:
        return "FIELD_MISMATCH:desk_code"
    return None


def _validate_notional_amount(row: dict) -> str | None:
    # LOGIC — rule 5: notional_amount must cast to float and be finite
    try:
        val = float(row["notional_amount"])
    except (ValueError, TypeError):
        return "INVALID_FORMAT:notional_amount"
    if not math.isfinite(val):
        return "INVALID_FORMAT:notional_amount"
    return None


def _validate_currency(row: dict) -> str | None:
    # LOGIC — rule 6: currency must be exactly 3 uppercase alpha characters (ISO 4217)
    currency_val = str(row["currency"]).strip()
    if not re.fullmatch(r"[A-Z]{3}", currency_val):
        return "INVALID_FORMAT:currency"
    return None


def _validate_counterparty_id(row: dict) -> str | None:
    # LOGIC — rule 7: counterparty_id must be a non-empty string (already checked in mandatory, belt-and-suspenders)
    if str(row["counterparty_id"]).strip() == "":
        return "INVALID_FORMAT:counterparty_id"
    return None


def _apply_rules(row: dict, filename_desk_code: str, filename_trade_date: date) -> str | None:
    # LOGIC — apply all validation rules in order; return first failure reason or None if all pass
    reason = _validate_mandatory_fields(row)
    if reason:
        return reason

    reason = _validate_trade_id(row)
    if reason:
        return reason

    reason = _validate_trade_date(row, filename_trade_date)
    if reason:
        return reason

    reason = _validate_desk_code(row, filename_desk_code)
    if reason:
        return reason

    reason = _validate_notional_amount(row)
    if reason:
        return reason

    reason = _validate_currency(row)
    if reason:
        return reason

    reason = _validate_counterparty_id(row)
    if reason:
        return reason

    return None


def validate_rows(
    raw_df: pd.DataFrame,
    filename_desk_code: str,
    filename_trade_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — main entry point: splits raw_df into valid and rejected DataFrames
    logger.info(
        "Starting row validation: total_rows=%d desk_code=%s trade_date=%s",
        len(raw_df),
        filename_desk_code,
        filename_trade_date,
    )

    valid_indices: list[int] = []
    rejected_rows: list[dict] = []

    for idx, row in raw_df.iterrows():
        row_dict = row.to_dict()
        rejection_reason = _apply_rules(row_dict, filename_desk_code, filename_trade_date)

        if rejection_reason is None:
            valid_indices.append(idx)
        else:
            # LOGIC — attach rejection_reason to a copy of the row dict
            rejected_row = row_dict.copy()
            rejected_row["rejection_reason"] = rejection_reason
            rejected_rows.append(rejected_row)

    # LOGIC — construct valid_df preserving original columns only
    if valid_indices:
        valid_df = raw_df.loc[valid_indices].reset_index(drop=True)
    else:
        valid_df = raw_df.iloc[0:0].reset_index(drop=True)

    # LOGIC — construct rejected_df with original columns + rejection_reason
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows, columns=list(raw_df.columns) + ["rejection_reason"])
    else:
        empty_cols = list(raw_df.columns) + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=empty_cols)

    logger.info(
        "Validation complete: valid_rows=%d rejected_rows=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df