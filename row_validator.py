# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered validation rules per the design specification
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

_FIELD_MAX_LENGTHS = {
    "trade_id": 100,
    "desk_code": 50,
    "counterparty_id": 100,
    "instrument_type": 100,
}


def _check_missing_mandatory(row: pd.Series) -> str | None:
    # LOGIC — first-fail: any mandatory field empty string or absent
    for field in _MANDATORY_FIELDS:
        val = row.get(field, "")
        if val is None or str(val).strip() == "":
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date_format(val: str) -> str | None:
    # LOGIC — must parse as YYYY-MM-DD
    try:
        datetime.strptime(val.strip(), "%Y-%m-%d")
    except ValueError:
        return f"Invalid trade_date format: {val}"
    return None


def _check_notional_amount(val: str) -> str | None:
    # LOGIC — must be parseable as Decimal and non-negative
    stripped = val.strip()
    try:
        amount = Decimal(stripped)
    except InvalidOperation:
        return f"Invalid notional_amount: not numeric"
    if amount < Decimal("0"):
        return f"Invalid notional_amount: negative value {stripped}"
    return None


def _check_currency_format(val: str) -> str | None:
    # LOGIC — exactly 3 uppercase alpha characters
    if not _CURRENCY_RE.match(val.strip()):
        return f"Invalid currency format: {val}"
    return None


def _check_field_length(row: pd.Series) -> str | None:
    # LOGIC — length limits for string fields
    for field, max_len in _FIELD_MAX_LENGTHS.items():
        val = str(row.get(field, ""))
        if len(val) > max_len:
            return f"Field too long: {field} (max {max_len})"
    return None


def _get_rejection_reason(row: pd.Series) -> str | None:
    # LOGIC — apply checks in order; first failure wins
    reason = _check_missing_mandatory(row)
    if reason:
        return reason

    reason = _check_trade_date_format(str(row["trade_date"]))
    if reason:
        return reason

    reason = _check_notional_amount(str(row["notional_amount"]))
    if reason:
        return reason

    reason = _check_currency_format(str(row["currency"]))
    if reason:
        return reason

    reason = _check_field_length(row)
    if reason:
        return reason

    return None


def _cast_valid_row(row: pd.Series) -> pd.Series:
    # LOGIC — type-cast notional_amount to Decimal and trade_date to datetime.date
    row = row.copy()
    row["notional_amount"] = Decimal(str(row["notional_amount"]).strip())
    row["trade_date"] = datetime.strptime(
        str(row["trade_date"]).strip(), "%Y-%m-%d"
    ).date()
    return row


def validate_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — partition rows into valid and rejected sets
    if df.empty:
        logger.info("validate_rows received an empty DataFrame; returning empty valid and rejected sets")
        empty_rejected = df.copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), empty_rejected

    # LOGIC — ensure all expected columns are present; treat missing columns as empty strings
    for field in _MANDATORY_FIELDS:
        if field not in df.columns:
            logger.warning("Expected column '%s' not found in DataFrame; adding as empty", field)
            df = df.copy()
            df[field] = ""

    valid_rows = []
    rejected_rows = []

    for idx, row in df.iterrows():
        reason = _get_rejection_reason(row)
        if reason is None:
            # LOGIC — cast to proper Python types for downstream DB insert
            try:
                valid_rows.append(_cast_valid_row(row))
            except Exception as cast_exc:  # noqa: BLE001
                logger.error(
                    "Row %s passed validation but failed type-cast: %s", idx, cast_exc
                )
                rejected_row = row.copy()
                rejected_row["rejection_reason"] = f"Type-cast failure: {cast_exc}"
                rejected_rows.append(rejected_row)
        else:
            rejected_row = row.copy()
            rejected_row["rejection_reason"] = reason
            rejected_rows.append(rejected_row)

    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
        # LOGIC — re-apply casted types column-wise after DataFrame reconstruction
        valid_df["notional_amount"] = valid_df["notional_amount"].apply(
            lambda v: v if isinstance(v, Decimal) else Decimal(str(v).strip())
        )
        valid_df["trade_date"] = valid_df["trade_date"].apply(
            lambda v: v if isinstance(v, __import__("datetime").date) else
            datetime.strptime(str(v).strip(), "%Y-%m-%d").date()
        )
    else:
        valid_df = pd.DataFrame(columns=df.columns.tolist())

    if rejected_rows:
        rejected_cols = df.columns.tolist() + ["rejection_reason"]
        rejected_df = pd.DataFrame(rejected_rows, columns=rejected_cols)
    else:
        rejected_cols = df.columns.tolist() + ["rejection_reason"]
        rejected_df = pd.DataFrame(columns=rejected_cols)

    logger.info(
        "validate_rows: total=%d valid=%d rejected=%d",
        len(df),
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df