# BOILERPLATE
import logging
import re
from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns that must be present and non-empty
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_null_or_empty(value) -> bool:
    # LOGIC — treats pandas NA, None, and blank strings as missing
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _validate_trade_id(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "trade_id: must be a non-empty string"
    return None


def _validate_desk_code(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "desk_code: must be a non-empty string"
    return None


def _validate_trade_date(value) -> str | None:
    # LOGIC — must be parseable as YYYY-MM-DD
    if _is_null_or_empty(value):
        return "trade_date: must be a non-empty date string"
    try:
        pd.to_datetime(str(value).strip(), format="%Y-%m-%d")
        return None
    except (ValueError, TypeError):
        return "trade_date: must be a valid date in YYYY-MM-DD format"


def _validate_instrument_type(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "instrument_type: must be a non-empty string"
    return None


def _validate_notional_amount(value) -> str | None:
    # LOGIC — must be castable to a decimal number
    if _is_null_or_empty(value):
        return "notional_amount: must be a non-empty decimal number"
    try:
        Decimal(str(value).strip())
        return None
    except InvalidOperation:
        return "notional_amount: not a valid decimal"


def _validate_currency(value) -> str | None:
    # LOGIC — must be exactly 3 alphabetic characters
    if _is_null_or_empty(value):
        return "currency: must be exactly 3 alphabetic characters"
    cleaned = str(value).strip()
    if len(cleaned) != 3 or not cleaned.isalpha():
        return "currency: must be exactly 3 alphabetic characters"
    return None


def _validate_counterparty_id(value) -> str | None:
    # LOGIC
    if _is_null_or_empty(value):
        return "counterparty_id: must be a non-empty string"
    return None


# LOGIC — dispatch table mapping column name to its validator function
_VALIDATORS = {
    "trade_id": _validate_trade_id,
    "desk_code": _validate_desk_code,
    "trade_date": _validate_trade_date,
    "instrument_type": _validate_instrument_type,
    "notional_amount": _validate_notional_amount,
    "currency": _validate_currency,
    "counterparty_id": _validate_counterparty_id,
}


def _collect_row_errors(row: pd.Series) -> list[str]:
    # LOGIC — run all validators for a single row; collect all failures
    errors = []
    for col, validator_fn in _VALIDATORS.items():
        raw_value = row.get(col)
        error_msg = validator_fn(raw_value)
        if error_msg is not None:
            errors.append(error_msg)
    return errors


def _cast_valid_row(row: pd.Series) -> dict:
    # LOGIC — cast columns to their target types for a row that passed all validations
    return {
        "trade_id": str(row["trade_id"]).strip(),
        "desk_code": str(row["desk_code"]).strip(),
        "trade_date": pd.to_datetime(str(row["trade_date"]).strip(), format="%Y-%m-%d").date(),
        "instrument_type": str(row["instrument_type"]).strip(),
        "notional_amount": Decimal(str(row["notional_amount"]).strip()),
        "currency": str(row["currency"]).strip().upper(),
        "counterparty_id": str(row["counterparty_id"]).strip(),
    }


def validate_rows(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — entry point: split raw DataFrame into valid and rejected subsets
    if raw_df.empty:
        logger.info("validate_rows: received empty DataFrame — returning empty valid and rejected frames")
        valid_df = pd.DataFrame(columns=_MANDATORY_COLUMNS)
        rejected_df = pd.DataFrame(columns=_MANDATORY_COLUMNS + ["rejection_reason"])
        return valid_df, rejected_df

    # LOGIC — ensure all mandatory columns exist; add as null if missing so validators can report them
    for col in _MANDATORY_COLUMNS:
        if col not in raw_df.columns:
            logger.warning("validate_rows: mandatory column '%s' missing from input — treating as null", col)
            raw_df = raw_df.copy()
            raw_df[col] = None

    valid_records: list[dict] = []
    rejected_records: list[dict] = []

    for idx, row in raw_df.iterrows():
        # LOGIC — collect all errors for this row in one pass
        errors = _collect_row_errors(row)

        if errors:
            # LOGIC — build rejected record: preserve all original columns + add rejection_reason
            rejected_record = row.to_dict()
            rejected_record["rejection_reason"] = " | ".join(errors)
            rejected_records.append(rejected_record)
            logger.debug(
                "validate_rows: row index %s rejected — reasons: %s",
                idx,
                rejected_record["rejection_reason"],
            )
        else:
            # LOGIC — cast to target types before storing in valid set
            valid_record = _cast_valid_row(row)
            valid_records.append(valid_record)

    # LOGIC — build output DataFrames
    if valid_records:
        valid_df = pd.DataFrame(valid_records)
    else:
        valid_df = pd.DataFrame(columns=_MANDATORY_COLUMNS)

    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
    else:
        rejected_df = pd.DataFrame(columns=list(raw_df.columns) + ["rejection_reason"])

    logger.info(
        "validate_rows: total=%d  valid=%d  rejected=%d",
        len(raw_df),
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df