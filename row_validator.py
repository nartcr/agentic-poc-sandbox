# BOILERPLATE
import logging
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered list of mandatory columns per data contract
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — currency regex per TAC-2
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# LOGIC — trade date format per TAC-2
_TRADE_DATE_FMT = "%Y-%m-%d"


def _check_mandatory_fields(df: pd.DataFrame) -> pd.Series:
    # LOGIC
    # Returns a boolean Series: True where ALL mandatory fields are non-null
    # and non-empty after strip.  Also populates a per-row reason fragment
    # via a helper so validate() can retrieve it.  Because Series can only
    # carry one value per row we return the mask here; reason accumulation
    # is handled inside validate() which calls this alongside reason building.
    valid_mask = pd.Series(True, index=df.index)
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            # Column entirely absent — every row fails this field
            valid_mask = pd.Series(False, index=df.index)
        else:
            col = df[field].fillna("").astype(str).str.strip()
            valid_mask = valid_mask & (col != "")
    return valid_mask


def _check_trade_date_format(df: pd.DataFrame) -> pd.Series:
    # LOGIC — True where trade_date parses successfully as YYYY-MM-DD
    def _is_valid_date(val: str) -> bool:
        if pd.isna(val):
            return False
        try:
            datetime.strptime(str(val).strip(), _TRADE_DATE_FMT)
            return True
        except ValueError:
            return False

    return df["trade_date"].apply(_is_valid_date)


def _check_notional_amount(df: pd.DataFrame) -> pd.Series:
    # LOGIC — True where notional_amount is castable to float AND > 0
    def _is_valid_notional(val: str) -> bool:
        if pd.isna(val):
            return False
        try:
            return float(str(val).strip()) > 0
        except (ValueError, TypeError):
            return False

    return df["notional_amount"].apply(_is_valid_notional)


def _check_currency_format(df: pd.DataFrame) -> pd.Series:
    # LOGIC — True where currency matches ^[A-Z]{3}$
    def _is_valid_currency(val: str) -> bool:
        if pd.isna(val):
            return False
        return bool(_CURRENCY_RE.match(str(val).strip()))

    return df["currency"].apply(_is_valid_currency)


def _check_intrafile_duplicates(df: pd.DataFrame) -> pd.Series:
    # LOGIC — True where the (trade_id, desk_code, trade_date) tuple is unique
    # within the file.  All occurrences of a duplicated tuple are marked False.
    duplicate_mask = df.duplicated(
        subset=["trade_id", "desk_code", "trade_date"], keep=False
    )
    return ~duplicate_mask  # True = not a duplicate = valid


def validate(df: pd.DataFrame) -> tuple:
    # LOGIC — orchestrates all five validation rules in order, accumulates
    # rejection reasons per row, then splits into valid_df and rejected_df.

    if df.empty:
        logger.info("validate() received empty DataFrame; returning empty splits.")
        empty_rejected = df.copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), empty_rejected

    # LOGIC — initialise per-row reason lists (one list per row index)
    reasons: dict = {idx: [] for idx in df.index}

    # ------------------------------------------------------------------ #
    # Rule 1 — mandatory field presence                                    #
    # ------------------------------------------------------------------ #
    # LOGIC — we need per-field failure info so we iterate fields directly
    # rather than delegating entirely to _check_mandatory_fields.
    mandatory_valid_mask = pd.Series(True, index=df.index)
    for field in MANDATORY_FIELDS:
        if field not in df.columns:
            field_invalid = pd.Series(True, index=df.index)  # all rows fail
        else:
            col = df[field].fillna("").astype(str).str.strip()
            field_invalid = col == ""

        # LOGIC — accumulate reason for rows where this specific field fails
        for idx in df.index[field_invalid]:
            reasons[idx].append(f"missing_mandatory_field:{field}")

        mandatory_valid_mask = mandatory_valid_mask & ~field_invalid

    # ------------------------------------------------------------------ #
    # Rule 2 — trade_date format                                           #
    # ------------------------------------------------------------------ #
    date_valid_mask = _check_trade_date_format(df)
    # LOGIC — only flag rows that are not already flagged for missing trade_date
    # (avoid double-reporting the same field with two different reason codes)
    date_format_failed = ~date_valid_mask
    for idx in df.index[date_format_failed]:
        # Only add format reason if the value is actually present (not empty)
        val = str(df.at[idx, "trade_date"]).strip() if "trade_date" in df.columns else ""
        if val and val.lower() not in ("nan", "none", ""):
            reasons[idx].append("invalid_trade_date_format")

    # ------------------------------------------------------------------ #
    # Rule 3 — notional_amount numeric and > 0                            #
    # ------------------------------------------------------------------ #
    notional_valid_mask = _check_notional_amount(df)
    notional_failed = ~notional_valid_mask
    for idx in df.index[notional_failed]:
        val = str(df.at[idx, "notional_amount"]).strip() if "notional_amount" in df.columns else ""
        if val and val.lower() not in ("nan", "none", ""):
            reasons[idx].append("invalid_notional_amount")

    # ------------------------------------------------------------------ #
    # Rule 4 — currency format                                            #
    # ------------------------------------------------------------------ #
    currency_valid_mask = _check_currency_format(df)
    currency_failed = ~currency_valid_mask
    for idx in df.index[currency_failed]:
        val = str(df.at[idx, "currency"]).strip() if "currency" in df.columns else ""
        if val and val.lower() not in ("nan", "none", ""):
            reasons[idx].append("invalid_currency_format")

    # ------------------------------------------------------------------ #
    # Rule 5 — intra-file duplicates                                      #
    # ------------------------------------------------------------------ #
    dedup_valid_mask = _check_intrafile_duplicates(df)
    dedup_failed = ~dedup_valid_mask
    for idx in df.index[dedup_failed]:
        reasons[idx].append("duplicate_within_file")

    # ------------------------------------------------------------------ #
    # LOGIC — compute overall valid/invalid masks                         #
    # ------------------------------------------------------------------ #
    # A row is overall-valid only when it has zero reason strings
    overall_invalid_mask = pd.Series(
        {idx: len(reason_list) > 0 for idx, reason_list in reasons.items()},
        dtype=bool,
    )
    overall_valid_mask = ~overall_invalid_mask

    # ------------------------------------------------------------------ #
    # LOGIC — split DataFrames                                            #
    # ------------------------------------------------------------------ #
    valid_df = df.loc[overall_valid_mask].copy()
    valid_df = valid_df.reset_index(drop=True)

    rejected_rows = df.loc[overall_invalid_mask].copy()
    rejected_rows["rejection_reason"] = rejected_rows.index.map(
        lambda idx: " | ".join(reasons[idx])
    )
    rejected_df = rejected_rows.reset_index(drop=True)

    logger.info(
        "Validation complete: total=%d, valid=%d, rejected=%d",
        len(df),
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df