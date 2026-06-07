# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns as specified in the data contract
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_blank(value: str) -> bool:
    # LOGIC — treat None, NaN coerced to string "nan", and whitespace-only as blank
    if value is None:
        return True
    if not isinstance(value, str):
        return True
    return value.strip() == "" or value.strip().lower() == "nan"


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — rule 1: missing mandatory fields, checked in column order
    for field in MANDATORY_FIELDS:
        value = row.get(field, None)
        if _is_blank(value):
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date_format(row: pd.Series) -> str | None:
    # LOGIC — rule 2: trade_date must parse as YYYY-MM-DD
    value = str(row["trade_date"]).strip()
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return f"Invalid trade_date format: {value}"
    return None


def _check_notional_amount(row: pd.Series) -> str | None:
    # LOGIC — rule 3: notional_amount must parse as a valid decimal number
    value = str(row["notional_amount"]).strip()
    try:
        Decimal(value)
    except InvalidOperation:
        return f"Non-numeric notional_amount: {value}"
    return None


def _check_currency_length(row: pd.Series) -> str | None:
    # LOGIC — rule 4: currency must be exactly 3 characters after stripping whitespace
    value = str(row["currency"]).strip()
    if len(value) != 3:
        return f"Invalid currency length: {value}"
    return None


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — main validation entry point; applies all five rules in order
    # Returns (valid_df, rejected_df); rejected_df carries rejection_reason column
    logger.info("Starting row validation on %d rows", len(df))

    valid_indices: list[int] = []
    rejected_rows: list[dict] = []

    # LOGIC — track seen composite keys for intra-file duplicate detection (rule 5)
    seen_composite_keys: set[tuple[str, str, str]] = set()

    for idx, row in df.iterrows():
        rejection_reason: str | None = None

        # LOGIC — rule 1: mandatory fields
        rejection_reason = _check_mandatory_fields(row)

        # LOGIC — rule 2: trade_date format (only if mandatory check passed)
        if rejection_reason is None:
            rejection_reason = _check_trade_date_format(row)

        # LOGIC — rule 3: notional_amount numeric (only if prior checks passed)
        if rejection_reason is None:
            rejection_reason = _check_notional_amount(row)

        # LOGIC — rule 4: currency length (only if prior checks passed)
        if rejection_reason is None:
            rejection_reason = _check_currency_length(row)

        # LOGIC — rule 5: intra-file duplicate detection on (trade_id, desk_code, trade_date)
        if rejection_reason is None:
            trade_id_val = str(row["trade_id"]).strip()
            desk_code_val = str(row["desk_code"]).strip()
            trade_date_val = str(row["trade_date"]).strip()
            composite_key = (trade_id_val, desk_code_val, trade_date_val)
            if composite_key in seen_composite_keys:
                rejection_reason = f"Duplicate trade_id within file: {trade_id_val}"
            else:
                seen_composite_keys.add(composite_key)

        if rejection_reason is None:
            # LOGIC — row passed all validation rules
            valid_indices.append(idx)
        else:
            # LOGIC — row failed at least one rule; record it with reason
            rejected_row = row.to_dict()
            rejected_row["rejection_reason"] = rejection_reason
            rejected_rows.append(rejected_row)

    # LOGIC — build valid DataFrame from surviving indices
    if valid_indices:
        valid_df = df.loc[valid_indices].reset_index(drop=True)
    else:
        valid_df = df.iloc[0:0].copy().reset_index(drop=True)

    # LOGIC — build rejected DataFrame from accumulated rejected row dicts
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows, columns=list(df.columns) + ["rejection_reason"])
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        rejected_df = df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        rejected_df = rejected_df.reset_index(drop=True)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df