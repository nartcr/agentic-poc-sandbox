# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of the 7 mandatory field names per the data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — currency must be exactly 3 alphabetic characters
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")

# LOGIC — trade_date must be parseable as YYYY-MM-DD
_DATE_FMT = "%Y-%m-%d"


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — iterate mandatory fields in declared order; return the first missing field's reason
    for field in _MANDATORY_FIELDS:
        value = row.get(field)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return f"missing: {field}"
        if isinstance(value, str) and value.strip() == "":
            return f"missing: {field}"
    return None


def _check_field_formats(row: pd.Series) -> str | None:
    # LOGIC — validate trade_date format: must parse as YYYY-MM-DD
    trade_date_val = row.get("trade_date")
    if trade_date_val is not None and isinstance(trade_date_val, str) and trade_date_val.strip() != "":
        try:
            datetime.strptime(trade_date_val.strip(), _DATE_FMT)
        except ValueError:
            return f"invalid trade_date format: {trade_date_val}"

    # LOGIC — validate notional_amount: must be parseable as a finite decimal
    notional_val = row.get("notional_amount")
    if notional_val is not None and isinstance(notional_val, str) and notional_val.strip() != "":
        try:
            parsed = Decimal(notional_val.strip())
            # LOGIC — reject special Decimal values: Infinity, -Infinity, NaN
            if not parsed.is_finite():
                return f"non-numeric notional_amount: {notional_val}"
        except InvalidOperation:
            return f"non-numeric notional_amount: {notional_val}"

    # LOGIC — validate currency: exactly 3 alphabetic characters
    currency_val = row.get("currency")
    if currency_val is not None and isinstance(currency_val, str) and currency_val.strip() != "":
        if not _CURRENCY_RE.match(currency_val.strip()):
            return f"invalid currency format: {currency_val}"

    return None


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # BOILERPLATE — initialise result tracking lists
    valid_indices = []
    rejected_indices = []
    rejection_reasons = {}

    # LOGIC — iterate every row; apply mandatory check first, then format check
    for idx, row in df.iterrows():
        reason = _check_mandatory_fields(row)
        if reason is None:
            reason = _check_field_formats(row)

        if reason is not None:
            rejected_indices.append(idx)
            rejection_reasons[idx] = reason
            logger.debug(
                "Row %s rejected: %s",
                idx,
                reason,
            )
        else:
            valid_indices.append(idx)

    # LOGIC — build valid DataFrame from passing indices; reset index for clean downstream use
    valid_df = df.loc[valid_indices].copy().reset_index(drop=True)

    # LOGIC — build rejected DataFrame; append rejection_reason column
    if rejected_indices:
        rejected_df = df.loc[rejected_indices].copy()
        rejected_df["rejection_reason"] = pd.Series(rejection_reasons)
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        # LOGIC — return empty rejected DataFrame with rejection_reason column present
        rejected_df = df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df