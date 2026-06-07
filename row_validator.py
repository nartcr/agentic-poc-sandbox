# BOILERPLATE
import re
import logging
import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns as specified in the data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")


def _check_mandatory_fields(row: pd.Series) -> list[str]:
    # LOGIC — rule 1: every mandatory field must be non-null and non-empty string
    reasons: list[str] = []
    for field in _MANDATORY_FIELDS:
        value = row.get(field)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            reasons.append(f"missing {field}")
            continue
        # treat whitespace-only strings as missing
        if isinstance(value, str) and value.strip() == "":
            reasons.append(f"missing {field}")
    return reasons


def _check_trade_date(row: pd.Series) -> list[str]:
    # LOGIC — rule 2: trade_date must parse as YYYY-MM-DD
    reasons: list[str] = []
    value = row.get("trade_date")
    if value is None or (isinstance(value, float) and pd.isna(value)):
        # already caught by mandatory-field check; skip to avoid duplicate reason
        return reasons
    if isinstance(value, (datetime.date, datetime.datetime)):
        # pandas may have already parsed it; treat as valid
        return reasons
    try:
        datetime.datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except ValueError:
        reasons.append("invalid trade_date format")
    return reasons


def _check_notional_amount(row: pd.Series) -> list[str]:
    # LOGIC — rule 3: notional_amount must be a positive non-zero float
    reasons: list[str] = []
    value = row.get("notional_amount")
    if value is None or (isinstance(value, float) and pd.isna(value)):
        # already caught by mandatory-field check; skip duplicate
        return reasons
    try:
        amount = float(value)
    except (ValueError, TypeError):
        reasons.append("invalid notional_amount format")
        return reasons
    if amount <= 0:
        reasons.append("notional_amount must be positive and non-zero")
    return reasons


def _check_currency(row: pd.Series) -> list[str]:
    # LOGIC — rule 4: currency must be exactly 3 alphabetic characters
    reasons: list[str] = []
    value = row.get("currency")
    if value is None or (isinstance(value, float) and pd.isna(value)):
        # already caught by mandatory-field check; skip duplicate
        return reasons
    if not _CURRENCY_RE.match(str(value).strip()):
        reasons.append("invalid currency format")
    return reasons


def _check_desk_code(row: pd.Series, expected: str) -> list[str]:
    # LOGIC — rule 5: desk_code in the row must match the desk_code parsed from the filename
    reasons: list[str] = []
    value = row.get("desk_code")
    if value is None or (isinstance(value, float) and pd.isna(value)):
        # already caught by mandatory-field check; skip duplicate
        return reasons
    actual = str(value).strip()
    if actual != expected:
        reasons.append(f"desk_code mismatch: expected {expected}, got {actual}")
    return reasons


def validate_rows(
    df: pd.DataFrame,
    expected_desk_code: str,
    expected_trade_date: datetime.date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — orchestrator: apply all checks to every row; split into valid and rejected
    if df.empty:
        logger.info("Input DataFrame is empty; returning empty valid and rejected DataFrames.")
        empty_valid = df.copy().reset_index(drop=True)
        empty_rejected = df.copy()
        if "rejection_reason" not in empty_rejected.columns:
            empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return empty_valid, empty_rejected.reset_index(drop=True)

    logger.info(
        "Starting row validation: %d rows, expected_desk_code=%s, expected_trade_date=%s",
        len(df),
        expected_desk_code,
        expected_trade_date,
    )

    rejection_reasons: list[str | None] = []

    for idx, row in df.iterrows():
        # LOGIC — collect all failure reasons across all rules in one pass
        all_reasons: list[str] = []
        all_reasons.extend(_check_mandatory_fields(row))
        all_reasons.extend(_check_trade_date(row))
        all_reasons.extend(_check_notional_amount(row))
        all_reasons.extend(_check_currency(row))
        all_reasons.extend(_check_desk_code(row, expected_desk_code))

        if all_reasons:
            rejection_reasons.append("; ".join(all_reasons))
        else:
            rejection_reasons.append(None)

    # LOGIC — build a boolean mask for rows with no rejection reasons
    rejection_series = pd.Series(rejection_reasons, index=df.index)
    valid_mask = rejection_series.isna()

    valid_df = df.loc[valid_mask].copy().reset_index(drop=True)

    rejected_df = df.loc[~valid_mask].copy()
    rejected_df["rejection_reason"] = rejection_series.loc[~valid_mask].values
    rejected_df = rejected_df.reset_index(drop=True)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows.",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df