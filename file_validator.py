# BOILERPLATE
import io
import math
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory field names as specified in the data contracts
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — maximum field lengths per design spec
FIELD_MAX_LENGTHS = {
    "trade_id": 100,
    "desk_code": 50,
    "instrument_type": 100,
    "counterparty_id": 100,
}


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — Rule 1: all seven mandatory fields present and non-empty
    for field in MANDATORY_FIELDS:
        if field not in row.index:
            return f"Missing mandatory field: {field}"
        value = row[field]
        if pd.isna(value) or str(value).strip() == "":
            return f"Missing mandatory field: {field}"
    return None


def _check_trade_date(row: pd.Series) -> str | None:
    # LOGIC — Rule 2: trade_date parseable as YYYY-MM-DD
    value = str(row["trade_date"]).strip()
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return f"Invalid trade_date: {value}"
    return None


def _check_notional_amount(row: pd.Series) -> str | None:
    # LOGIC — Rule 3: notional_amount parseable as finite decimal (not NaN, not infinite)
    value = str(row["notional_amount"]).strip()
    try:
        parsed = float(value)
        if not math.isfinite(parsed):
            return f"Invalid notional_amount: {value}"
    except ValueError:
        return f"Invalid notional_amount: {value}"
    return None


def _check_currency(row: pd.Series) -> str | None:
    # LOGIC — Rule 4: currency is exactly 3 alpha characters
    value = str(row["currency"]).strip()
    if len(value) != 3 or not value.isalpha():
        return f"Invalid currency: {value}"
    return None


def _check_field_lengths(row: pd.Series) -> str | None:
    # LOGIC — Rule 5: field length caps
    for field, max_len in FIELD_MAX_LENGTHS.items():
        value = str(row[field]).strip()
        if len(value) > max_len:
            return f"Field too long: {field} (length {len(value)}, max {max_len})"
    return None


def _validate_row(row: pd.Series) -> str | None:
    # LOGIC — apply all validation rules in order; return first failure reason or None
    reason = _check_mandatory_fields(row)
    if reason:
        return reason

    reason = _check_trade_date(row)
    if reason:
        return reason

    reason = _check_notional_amount(row)
    if reason:
        return reason

    reason = _check_currency(row)
    if reason:
        return reason

    reason = _check_field_lengths(row)
    if reason:
        return reason

    return None


def validate_rows(
    csv_content: str,
    expected_desk_code: str,
    expected_trade_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — parse CSV from string content
    try:
        df = pd.read_csv(io.StringIO(csv_content), dtype=str, keep_default_na=False)
    except Exception as exc:
        logger.error("Failed to parse CSV content: %s", exc)
        raise ValueError(f"CSV parse error: {exc}") from exc

    logger.info(
        "Parsed CSV: %d rows, columns: %s", len(df), list(df.columns)
    )

    # LOGIC — check that all mandatory columns exist in the file header
    missing_columns = [f for f in MANDATORY_FIELDS if f not in df.columns]
    if missing_columns:
        raise ValueError(
            f"CSV missing required columns: {missing_columns}"
        )

    valid_rows = []
    rejected_rows = []

    for idx, row in df.iterrows():
        reason = _validate_row(row)
        if reason is None:
            valid_rows.append(row[MANDATORY_FIELDS])
        else:
            rejected_row = row.copy()
            rejected_row["rejection_reason"] = reason
            rejected_rows.append(rejected_row)

    # LOGIC — build valid DataFrame with canonical column order
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=MANDATORY_FIELDS)
        # LOGIC — coerce notional_amount to numeric for downstream use
        valid_df["notional_amount"] = pd.to_numeric(
            valid_df["notional_amount"], errors="coerce"
        )
        valid_df = valid_df.reset_index(drop=True)
    else:
        valid_df = pd.DataFrame(columns=MANDATORY_FIELDS)
        valid_df["notional_amount"] = pd.to_numeric(valid_df["notional_amount"])

    # LOGIC — build rejected DataFrame preserving all original columns plus rejection_reason
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows).reset_index(drop=True)
    else:
        rejected_df = pd.DataFrame(
            columns=list(df.columns) + ["rejection_reason"]
        )

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df