# BOILERPLATE
import logging
import re
from datetime import datetime
from typing import Tuple

import pandas as pd

from exceptions import ValidationError

logger = logging.getLogger(__name__)

# LOGIC — constants
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
_DATE_FORMAT = "%Y-%m-%d"

_VALID_OUTPUT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_REJECTED_EXTRA_COLUMN = "rejection_reason"


# LOGIC
def _check_mandatory_fields(row: pd.Series) -> str | None:
    """
    Rule 1: All mandatory fields must be non-null and non-empty strings.
    Returns rejection reason string if invalid, else None.
    """
    for field in _MANDATORY_FIELDS:
        value = row.get(field, None)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return f"Missing mandatory field: {field}"
    return None


# LOGIC
def _check_trade_date(row: pd.Series) -> str | None:
    """
    Rule 2: trade_date must parse as YYYY-MM-DD.
    Returns rejection reason string if invalid, else None.
    """
    value = row.get("trade_date", "")
    try:
        datetime.strptime(value.strip(), _DATE_FORMAT)
    except (ValueError, AttributeError):
        return f"Invalid trade_date format: {value}"
    return None


# LOGIC
def _check_notional_amount(row: pd.Series) -> str | None:
    """
    Rule 3: notional_amount must be castable to float and non-negative.
    Returns rejection reason string if invalid, else None.
    """
    value = row.get("notional_amount", "")
    try:
        numeric = float(value)
        if numeric < 0:
            raise ValueError("negative")
    except (ValueError, TypeError):
        return f"Invalid notional_amount: {value}"
    return None


# LOGIC
def _check_currency(row: pd.Series) -> str | None:
    """
    Rule 4: currency must be exactly 3 uppercase alphabetic characters.
    Returns rejection reason string if invalid, else None.
    """
    value = row.get("currency", "")
    if not isinstance(value, str) or not _CURRENCY_PATTERN.match(value.strip()):
        return f"Invalid currency: {value}"
    return None


# LOGIC
def validate_rows(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates each row of raw_df against five ordered business rules.
    Returns (valid_df, rejected_df).

    valid_df columns: trade_id, desk_code, trade_date, instrument_type,
                      notional_amount (float), currency, counterparty_id
    rejected_df columns: all original raw columns + rejection_reason
    """
    if raw_df.empty:
        logger.info("Input DataFrame is empty — returning empty valid and rejected frames.")
        valid_df = pd.DataFrame(columns=_VALID_OUTPUT_COLUMNS)
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)
        rejected_df = pd.DataFrame(columns=list(raw_df.columns) + [_REJECTED_EXTRA_COLUMN])
        return valid_df, rejected_df

    valid_rows = []       # list of dicts for accepted rows
    rejected_rows = []    # list of dicts for rejected rows (original row + reason)

    # LOGIC — Rule 5 state: track seen (trade_id, desk_code, trade_date) triples
    seen_triples: set[tuple] = set()

    for idx, row in raw_df.iterrows():
        rejection_reason: str | None = None

        # Rule 1: mandatory fields
        rejection_reason = _check_mandatory_fields(row)

        # Rule 2: trade_date format (only if passed Rule 1)
        if rejection_reason is None:
            rejection_reason = _check_trade_date(row)

        # Rule 3: notional_amount numeric and non-negative
        if rejection_reason is None:
            rejection_reason = _check_notional_amount(row)

        # Rule 4: currency ISO pattern
        if rejection_reason is None:
            rejection_reason = _check_currency(row)

        # Rule 5: intra-file duplicate detection
        if rejection_reason is None:
            trade_id = row["trade_id"].strip()
            desk_code = row["desk_code"].strip()
            trade_date = row["trade_date"].strip()
            triple = (trade_id, desk_code, trade_date)

            if triple in seen_triples:
                rejection_reason = (
                    f"Duplicate within file: trade_id={trade_id}, "
                    f"desk_code={desk_code}, trade_date={trade_date}"
                )
            else:
                seen_triples.add(triple)

        if rejection_reason is not None:
            # LOGIC — build rejected row: all original columns + reason
            rejected_row = row.to_dict()
            rejected_row[_REJECTED_EXTRA_COLUMN] = rejection_reason
            rejected_rows.append(rejected_row)
            logger.debug("Row %s rejected: %s", idx, rejection_reason)
        else:
            # LOGIC — build valid row with typed notional_amount
            valid_row = {
                "trade_id": row["trade_id"].strip(),
                "desk_code": row["desk_code"].strip(),
                "trade_date": row["trade_date"].strip(),
                "instrument_type": row["instrument_type"].strip(),
                "notional_amount": float(row["notional_amount"]),
                "currency": row["currency"].strip(),
                "counterparty_id": row["counterparty_id"].strip(),
            }
            valid_rows.append(valid_row)

    # LOGIC — assemble output DataFrames
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=_VALID_OUTPUT_COLUMNS)
    else:
        valid_df = pd.DataFrame(columns=_VALID_OUTPUT_COLUMNS)
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(float)

    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows)
        # LOGIC — ensure original column order is preserved, with rejection_reason last
        original_cols = [c for c in raw_df.columns if c != _REJECTED_EXTRA_COLUMN]
        rejected_df = rejected_df[original_cols + [_REJECTED_EXTRA_COLUMN]]
    else:
        rejected_df = pd.DataFrame(columns=list(raw_df.columns) + [_REJECTED_EXTRA_COLUMN])

    logger.info(
        "Validation complete — %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df