# BOILERPLATE
import logging
import re
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact rejection reason strings as defined in the data contract; order matches validation priority
_REQUIRED_FIELDS = [
    ("trade_id",        "trade_id is missing or empty"),
    ("desk_code",       "desk_code is missing or empty"),
    ("trade_date",      "trade_date is missing or empty"),
]

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

_REQUIRED_AFTER_DATE = [
    ("instrument_type", "instrument_type is missing or empty"),
    ("notional_amount", "notional_amount is missing or empty"),
]

_REQUIRED_AFTER_NOTIONAL = [
    ("currency",        "currency is missing or empty"),
]

_REQUIRED_AFTER_CURRENCY = [
    ("counterparty_id", "counterparty_id is missing or empty"),
]


def _is_missing(series: pd.Series) -> pd.Series:
    # LOGIC — a field is considered missing if it is NaN or an empty/whitespace-only string
    return series.isna() | (series.str.strip() == "")


def validate_rows(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # BOILERPLATE — work on a copy to avoid mutating the caller's DataFrame
    working = df.copy()

    # LOGIC — track which rows have already been assigned a rejection reason so that
    # first-failing-rule-wins semantics are enforced across all 10 rules
    rejection_reason = pd.Series("", index=working.index, dtype=str)
    already_rejected = pd.Series(False, index=working.index)

    def _apply_rule(mask: pd.Series, reason: str) -> None:
        # LOGIC — only assign the reason to rows that have not yet been rejected
        newly_failing = mask & ~already_rejected
        rejection_reason[newly_failing] = reason
        already_rejected[newly_failing] = True

    # LOGIC — Rule 1: trade_id required
    _apply_rule(_is_missing(working["trade_id"]) if "trade_id" in working.columns
                else pd.Series(True, index=working.index),
                "trade_id is missing or empty")

    # LOGIC — Rule 2: desk_code required
    _apply_rule(_is_missing(working["desk_code"]) if "desk_code" in working.columns
                else pd.Series(True, index=working.index),
                "desk_code is missing or empty")

    # LOGIC — Rule 3: trade_date required (presence check before format check)
    _apply_rule(_is_missing(working["trade_date"]) if "trade_date" in working.columns
                else pd.Series(True, index=working.index),
                "trade_date is missing or empty")

    # LOGIC — Rule 4: trade_date parseable as YYYY-MM-DD (only checked when trade_date is present)
    if "trade_date" in working.columns:
        date_present = ~_is_missing(working["trade_date"]) & ~already_rejected
        parsed_dates = pd.to_datetime(
            working.loc[date_present, "trade_date"],
            format="%Y-%m-%d",
            errors="coerce",
        )
        bad_date_format = date_present.copy()
        bad_date_format[date_present] = parsed_dates.isna()
        _apply_rule(bad_date_format, "trade_date is not a valid date (expected YYYY-MM-DD)")

    # LOGIC — Rule 5: instrument_type required
    _apply_rule(_is_missing(working["instrument_type"]) if "instrument_type" in working.columns
                else pd.Series(True, index=working.index),
                "instrument_type is missing or empty")

    # LOGIC — Rule 6: notional_amount required (presence check before numeric check)
    _apply_rule(_is_missing(working["notional_amount"]) if "notional_amount" in working.columns
                else pd.Series(True, index=working.index),
                "notional_amount is missing or empty")

    # LOGIC — Rule 7: notional_amount parseable as numeric (only checked when present)
    if "notional_amount" in working.columns:
        notional_present = ~_is_missing(working["notional_amount"]) & ~already_rejected
        numeric_values = pd.to_numeric(
            working.loc[notional_present, "notional_amount"],
            errors="coerce",
        )
        bad_numeric = notional_present.copy()
        bad_numeric[notional_present] = numeric_values.isna()
        _apply_rule(bad_numeric, "notional_amount is not numeric")

    # LOGIC — Rule 8: currency required (presence check before format check)
    _apply_rule(_is_missing(working["currency"]) if "currency" in working.columns
                else pd.Series(True, index=working.index),
                "currency is missing or empty")

    # LOGIC — Rule 9: currency must be exactly 3 uppercase letters (only checked when present)
    if "currency" in working.columns:
        currency_present = ~_is_missing(working["currency"]) & ~already_rejected
        bad_currency = currency_present.copy()
        bad_currency[currency_present] = ~working.loc[currency_present, "currency"].str.match(
            _CURRENCY_RE
        )
        _apply_rule(bad_currency, "currency must be a 3-letter ISO code")

    # LOGIC — Rule 10: counterparty_id required
    _apply_rule(_is_missing(working["counterparty_id"]) if "counterparty_id" in working.columns
                else pd.Series(True, index=working.index),
                "counterparty_id is missing or empty")

    # LOGIC — split into valid and rejected based on whether a rejection reason was assigned
    rejected_mask = already_rejected
    valid_mask = ~rejected_mask

    rejected_df = working.loc[rejected_mask].copy()
    rejected_df["rejection_reason"] = rejection_reason[rejected_mask].values

    valid_df = working.loc[valid_mask].copy()

    # LOGIC — cast notional_amount to float64 on valid rows as required by the data contract
    if len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        valid_df["notional_amount"] = valid_df["notional_amount"].astype("float64")

    # LOGIC — cast trade_date to datetime.date on valid rows to match Aurora DATE column type
    if len(valid_df) > 0 and "trade_date" in valid_df.columns:
        valid_df["trade_date"] = pd.to_datetime(
            valid_df["trade_date"], format="%Y-%m-%d"
        ).dt.date

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df