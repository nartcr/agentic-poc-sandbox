# BOILERPLATE
import logging
import math
import re
from datetime import datetime

import pandas as pd

from src.ingestion.exceptions import ValidationError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — canonical set of mandatory columns; order matches DATA CONTRACTS
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — ISO 4217 currency pattern: exactly 3 uppercase alpha characters
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# LOGIC — date format required by the design
_DATE_FORMAT = "%Y-%m-%d"


def _is_valid_date(value: str) -> bool:
    # LOGIC
    try:
        datetime.strptime(value, _DATE_FORMAT)
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_notional(value: str) -> bool:
    # LOGIC
    try:
        parsed = float(value)
        return math.isfinite(parsed)
    except (ValueError, TypeError):
        return False


def _validate_single_row(row: pd.Series, desk_code: str) -> list[str]:
    # LOGIC — collect all reasons; one call per row keeps each check independent
    reasons: list[str] = []

    # LOGIC — mandatory presence check for all 7 fields
    for col in _MANDATORY_COLUMNS:
        value = row.get(col, "")
        if not isinstance(value, str) or value.strip() == "":
            reasons.append(f"{col}: field is required and must not be empty")

    # LOGIC — format validations only if the field is present (non-empty)
    # to avoid duplicate/redundant messages for already-flagged empty fields

    trade_date_val = row.get("trade_date", "")
    if (
        isinstance(trade_date_val, str)
        and trade_date_val.strip() != ""
        and not _is_valid_date(trade_date_val.strip())
    ):
        reasons.append("trade_date: must match YYYY-MM-DD format")

    notional_val = row.get("notional_amount", "")
    if (
        isinstance(notional_val, str)
        and notional_val.strip() != ""
        and not _is_valid_notional(notional_val.strip())
    ):
        reasons.append("notional_amount: not a valid number")

    currency_val = row.get("currency", "")
    if (
        isinstance(currency_val, str)
        and currency_val.strip() != ""
        and not _CURRENCY_RE.match(currency_val.strip())
    ):
        reasons.append("currency: must be 3 uppercase letters")

    # LOGIC — cross-field consistency: row desk_code must match filename desk_code
    row_desk_code = row.get("desk_code", "")
    if (
        isinstance(row_desk_code, str)
        and row_desk_code.strip() != ""
        and row_desk_code.strip() != desk_code
    ):
        reasons.append(
            f"desk_code: row value '{row_desk_code.strip()}' does not match "
            f"filename desk_code '{desk_code}'"
        )

    return reasons


def validate_rows(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — guard: all mandatory columns must be present as headers
    missing_headers = [col for col in _MANDATORY_COLUMNS if col not in df.columns]
    if missing_headers:
        raise ValidationError(
            f"Input CSV is missing required columns: {missing_headers}"
        )

    logger.info(
        "Validating %d rows for desk_code='%s' trade_date='%s'",
        len(df),
        desk_code,
        trade_date,
    )

    # LOGIC — collect per-row validation results
    rejection_reasons: list[str] = []
    is_valid_mask: list[bool] = []

    for _, row in df.iterrows():
        reasons = _validate_single_row(row, desk_code)
        if reasons:
            rejection_reasons.append(" | ".join(reasons))
            is_valid_mask.append(False)
        else:
            rejection_reasons.append("")
            is_valid_mask.append(True)

    # BOILERPLATE — build boolean Series aligned with df index
    valid_series = pd.Series(is_valid_mask, index=df.index, dtype=bool)

    # LOGIC — split into valid and rejected subsets
    valid_df = df.loc[valid_series].copy()
    rejected_df = df.loc[~valid_series].copy()

    # LOGIC — attach rejection_reason to rejected rows
    reason_series = pd.Series(rejection_reasons, index=df.index, dtype=str)
    rejected_df["rejection_reason"] = reason_series.loc[~valid_series].values

    # LOGIC — cast notional_amount to float64 on valid rows as per DATA CONTRACTS
    if not valid_df.empty:
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df