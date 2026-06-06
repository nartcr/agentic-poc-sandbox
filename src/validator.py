# BOILERPLATE
import datetime
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of required fields; checked in this exact order
_REQUIRED_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — rejection reason strings must match the design exactly
_MISSING_REASON = "{field} is missing or empty"
_BAD_DATE_REASON = "trade_date is not a valid date (expected YYYY-MM-DD)"
_BAD_NOTIONAL_REASON = "notional_amount is not a valid number"


def _is_empty(value: str) -> bool:
    # LOGIC — treat None, NaN-like falsy, and whitespace-only strings as empty
    if value is None:
        return True
    if not isinstance(value, str):
        # pandas may coerce; guard defensively
        return pd.isna(value) if hasattr(pd, "isna") else False
    return value.strip() == ""


def _try_parse_date(value: str) -> datetime.date | None:
    # LOGIC — strict YYYY-MM-DD parse; returns None if invalid
    try:
        return datetime.datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def _try_parse_float(value: str) -> float | None:
    # LOGIC — returns None if value cannot be parsed as float
    try:
        return float(value.strip())
    except (ValueError, TypeError, AttributeError):
        return None


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — splits input DataFrame into (valid_df, rejected_df)
    # First-failure-wins per row; rejected rows carry _rejection_reason and _source_row_number

    valid_indices = []
    rejected_records = []  # list of (original_index, rejection_reason)

    for idx, row in df.iterrows():
        rejection_reason = _check_row(row)
        if rejection_reason is not None:
            rejected_records.append((idx, rejection_reason))
        else:
            valid_indices.append(idx)

    # LOGIC — build rejected_df with extra audit columns
    if rejected_records:
        rejected_indices = [r[0] for r in rejected_records]
        rejected_df = df.loc[rejected_indices].copy()
        rejected_df["_rejection_reason"] = [r[1] for r in rejected_records]
        # LOGIC — _source_row_number is 1-based: header = row 0, first data row = 1
        rejected_df["_source_row_number"] = [int(idx) + 1 for idx in rejected_indices]
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        # LOGIC — empty rejected_df retains the correct schema
        extra_cols = list(df.columns) + ["_rejection_reason", "_source_row_number"]
        rejected_df = pd.DataFrame(columns=extra_cols)

    # LOGIC — build valid_df with type casts applied
    if valid_indices:
        valid_df = df.loc[valid_indices].copy()
        # LOGIC — cast trade_date to datetime.date
        valid_df["trade_date"] = valid_df["trade_date"].apply(
            lambda v: _try_parse_date(v)
        )
        # LOGIC — cast notional_amount to float
        valid_df["notional_amount"] = valid_df["notional_amount"].apply(
            lambda v: _try_parse_float(v)
        )
        valid_df = valid_df.reset_index(drop=True)
    else:
        valid_df = pd.DataFrame(columns=list(df.columns))

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df


def _check_row(row: pd.Series) -> str | None:
    # LOGIC — applies all validation rules in order; returns first failing reason or None
    # Rule group 1: required field presence checks (in defined order)
    for field in _REQUIRED_FIELDS:
        value = row.get(field, None)
        if _is_empty(value):
            return _MISSING_REASON.format(field=field)

    # Rule: non-parseable date
    date_value = row.get("trade_date", "")
    if _try_parse_date(date_value) is None:
        return _BAD_DATE_REASON

    # Rule: non-numeric notional_amount
    notional_value = row.get("notional_amount", "")
    if _try_parse_float(notional_value) is None:
        return _BAD_NOTIONAL_REASON

    return None