# BOILERPLATE
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — compiled regex for ISO 4217 currency code: exactly 3 uppercase alpha
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")

# LOGIC — mandatory columns in the order they are validated
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def validate_rows(df: pd.DataFrame, desk_code: str, trade_date: str) -> tuple:
    # LOGIC
    # Validates each row against the 12 ordered rules.
    # First failing rule wins for each row.
    # Returns (valid_df, rejected_df).
    # rejected_df includes all original columns plus 'rejection_reason'.

    # Work on a copy; preserve original index for traceability
    working = df.copy()

    # Track which rows have already been assigned a rejection reason
    # (first-failing-rule semantics): start with all rows as "not yet rejected"
    rejection_reason = pd.Series("", index=working.index, dtype=str)
    already_rejected = pd.Series(False, index=working.index)

    # ------------------------------------------------------------------
    # Helper: apply a rejection label to rows where condition is True
    #         and the row has not already been rejected
    # ------------------------------------------------------------------
    def _flag(condition: pd.Series, label: str) -> None:
        # LOGIC
        newly_failed = condition & ~already_rejected
        rejection_reason[newly_failed] = label
        already_rejected[newly_failed] = True

    # ------------------------------------------------------------------
    # Rule 1: trade_id — must be non-null and non-empty string
    # ------------------------------------------------------------------
    # LOGIC
    trade_id_missing = (
        working["trade_id"].isna()
        | (working["trade_id"].astype(str).str.strip() == "")
    )
    _flag(trade_id_missing, "MISSING_TRADE_ID")

    # ------------------------------------------------------------------
    # Rule 2a: desk_code — must be non-null
    # ------------------------------------------------------------------
    # LOGIC
    desk_code_null = (
        working["desk_code"].isna()
        | (working["desk_code"].astype(str).str.strip() == "")
    )
    _flag(desk_code_null, "MISSING_DESK_CODE")

    # Rule 2b: desk_code — must match desk_code parsed from filename
    # LOGIC
    desk_code_mismatch = (
        ~already_rejected
        & (working["desk_code"].astype(str).str.strip() != desk_code)
    )
    _flag(desk_code_mismatch, "DESK_CODE_MISMATCH")

    # ------------------------------------------------------------------
    # Rule 3a: trade_date — must be non-null
    # ------------------------------------------------------------------
    # LOGIC
    trade_date_null = (
        working["trade_date"].isna()
        | (working["trade_date"].astype(str).str.strip() == "")
    )
    _flag(trade_date_null, "MISSING_TRADE_DATE")

    # Rule 3b: trade_date — must be parseable as YYYYMMDD date
    # LOGIC — use pd.to_datetime with format="%Y%m%d", errors="coerce"
    # Only check rows not yet rejected
    not_yet_rejected_mask = ~already_rejected
    if not_yet_rejected_mask.any():
        parsed_dates = pd.to_datetime(
            working.loc[not_yet_rejected_mask, "trade_date"].astype(str).str.strip(),
            format="%Y%m%d",
            errors="coerce",
        )
        invalid_date_format = not_yet_rejected_mask.copy()
        invalid_date_format[not_yet_rejected_mask] = parsed_dates.isna()
        _flag(invalid_date_format, "INVALID_TRADE_DATE_FORMAT")

    # Rule 3c: trade_date — must match trade_date parsed from filename
    # LOGIC
    not_yet_rejected_mask = ~already_rejected
    if not_yet_rejected_mask.any():
        trade_date_mismatch = not_yet_rejected_mask & (
            working["trade_date"].astype(str).str.strip() != trade_date
        )
        _flag(trade_date_mismatch, "TRADE_DATE_MISMATCH")

    # ------------------------------------------------------------------
    # Rule 4: instrument_type — must be non-null and non-empty string
    # ------------------------------------------------------------------
    # LOGIC
    instr_missing = (
        working["instrument_type"].isna()
        | (working["instrument_type"].astype(str).str.strip() == "")
    )
    _flag(instr_missing, "MISSING_INSTRUMENT_TYPE")

    # ------------------------------------------------------------------
    # Rule 5a: notional_amount — must be non-null
    # ------------------------------------------------------------------
    # LOGIC
    notional_null = (
        working["notional_amount"].isna()
        | (working["notional_amount"].astype(str).str.strip() == "")
    )
    _flag(notional_null, "MISSING_NOTIONAL_AMOUNT")

    # Rule 5b: notional_amount — must be castable to float AND > 0
    # LOGIC
    not_yet_rejected_mask = ~already_rejected
    if not_yet_rejected_mask.any():
        numeric_values = pd.to_numeric(
            working.loc[not_yet_rejected_mask, "notional_amount"].astype(str).str.strip(),
            errors="coerce",
        )
        # castable check: coerce produced NaN => not castable
        # value check: castable but <= 0 => invalid
        invalid_notional = not_yet_rejected_mask.copy()
        invalid_notional[not_yet_rejected_mask] = (
            numeric_values.isna() | (numeric_values <= 0)
        )
        _flag(invalid_notional, "INVALID_NOTIONAL_AMOUNT")

    # ------------------------------------------------------------------
    # Rule 6a: currency — must be non-null
    # ------------------------------------------------------------------
    # LOGIC
    currency_null = (
        working["currency"].isna()
        | (working["currency"].astype(str).str.strip() == "")
    )
    _flag(currency_null, "MISSING_CURRENCY")

    # Rule 6b: currency — must be exactly 3 uppercase alpha characters
    # LOGIC
    not_yet_rejected_mask = ~already_rejected
    if not_yet_rejected_mask.any():
        currency_vals = working.loc[not_yet_rejected_mask, "currency"].astype(str).str.strip()
        invalid_currency = not_yet_rejected_mask.copy()
        invalid_currency[not_yet_rejected_mask] = ~currency_vals.str.match(
            _CURRENCY_PATTERN.pattern
        )
        _flag(invalid_currency, "INVALID_CURRENCY_FORMAT")

    # ------------------------------------------------------------------
    # Rule 7: counterparty_id — must be non-null and non-empty string
    # ------------------------------------------------------------------
    # LOGIC
    cpty_missing = (
        working["counterparty_id"].isna()
        | (working["counterparty_id"].astype(str).str.strip() == "")
    )
    _flag(cpty_missing, "MISSING_COUNTERPARTY_ID")

    # ------------------------------------------------------------------
    # Split into valid and rejected DataFrames
    # ------------------------------------------------------------------
    # LOGIC
    rejected_mask = already_rejected
    valid_mask = ~rejected_mask

    valid_df = working.loc[valid_mask].copy()
    # Cast notional_amount to float in valid_df so downstream code gets numeric type
    if not valid_df.empty:
        valid_df["notional_amount"] = valid_df["notional_amount"].astype(str).str.strip().astype(float)

    rejected_df = working.loc[rejected_mask].copy()
    rejected_df["rejection_reason"] = rejection_reason[rejected_mask]

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows "
        "(desk_code=%s, trade_date=%s)",
        len(valid_df),
        len(rejected_df),
        desk_code,
        trade_date,
    )

    # Log a breakdown of rejection reasons for observability
    if not rejected_df.empty:
        reason_counts = rejected_df["rejection_reason"].value_counts().to_dict()
        logger.info("Rejection reason breakdown: %s", reason_counts)

    return valid_df, rejected_df