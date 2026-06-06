# BOILERPLATE
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")

_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_blank(series: pd.Series) -> pd.Series:
    # LOGIC — treat None, NaN, and whitespace-only strings as blank
    return series.isna() | series.str.strip().eq("")


def _is_valid_date(series: pd.Series) -> pd.Series:
    # LOGIC — must match YYYY-MM-DD pattern AND be parseable as a real date
    matches_pattern = series.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    parsed = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
    return matches_pattern & parsed.notna()


def _is_numeric(series: pd.Series) -> pd.Series:
    # LOGIC — attempt float cast; NaN result means not numeric
    return pd.to_numeric(series, errors="coerce").notna()


def _is_valid_currency(series: pd.Series) -> pd.Series:
    # LOGIC — exactly 3 uppercase alphabetic characters
    return series.str.match(r"^[A-Z]{3}$", na=False)


def validate(
    df: pd.DataFrame,
    expected_desk_code: str,
    expected_trade_date: str,
) -> tuple:
    # BOILERPLATE — work on a copy; never mutate the caller's DataFrame
    df = df.copy()

    # LOGIC — initialise a per-row list of rejection reasons
    n = len(df)
    reasons: list[list[str]] = [[] for _ in range(n)]

    # LOGIC — ensure all expected columns are present (add as empty if absent)
    for col in _REQUIRED_COLUMNS:
        if col not in df.columns:
            logger.warning("Expected column '%s' not found in DataFrame; filling with empty string.", col)
            df[col] = ""

    # LOGIC — cast all columns to str and strip leading/trailing whitespace
    for col in _REQUIRED_COLUMNS:
        df[col] = df[col].astype(str).replace("nan", "").str.strip()

    # ------------------------------------------------------------------
    # Check 1: trade_id is null/empty
    # ------------------------------------------------------------------
    mask = _is_blank(df["trade_id"])
    for i in mask[mask].index:
        reasons[df.index.get_loc(i)].append("trade_id is missing")

    # ------------------------------------------------------------------
    # Check 2: desk_code is null/empty
    # ------------------------------------------------------------------
    blank_desk = _is_blank(df["desk_code"])
    for i in blank_desk[blank_desk].index:
        reasons[df.index.get_loc(i)].append("desk_code is missing")

    # ------------------------------------------------------------------
    # Check 3: desk_code != expected_desk_code (only when non-blank)
    # ------------------------------------------------------------------
    mismatch_desk = (~blank_desk) & (df["desk_code"] != expected_desk_code)
    for i in mismatch_desk[mismatch_desk].index:
        reasons[df.index.get_loc(i)].append("desk_code does not match filename")

    # ------------------------------------------------------------------
    # Check 4: trade_date is null/empty
    # ------------------------------------------------------------------
    blank_date = _is_blank(df["trade_date"])
    for i in blank_date[blank_date].index:
        reasons[df.index.get_loc(i)].append("trade_date is missing")

    # ------------------------------------------------------------------
    # Check 5: trade_date not parseable as YYYY-MM-DD (only when non-blank)
    # ------------------------------------------------------------------
    valid_date_fmt = _is_valid_date(df["trade_date"])
    malformed_date = (~blank_date) & (~valid_date_fmt)
    for i in malformed_date[malformed_date].index:
        reasons[df.index.get_loc(i)].append("trade_date is malformed")

    # ------------------------------------------------------------------
    # Check 6: trade_date != expected_trade_date (only when non-blank and well-formed)
    # ------------------------------------------------------------------
    mismatch_date = (~blank_date) & valid_date_fmt & (df["trade_date"] != expected_trade_date)
    for i in mismatch_date[mismatch_date].index:
        reasons[df.index.get_loc(i)].append("trade_date does not match filename")

    # ------------------------------------------------------------------
    # Check 7: instrument_type is null/empty
    # ------------------------------------------------------------------
    blank_instr = _is_blank(df["instrument_type"])
    for i in blank_instr[blank_instr].index:
        reasons[df.index.get_loc(i)].append("instrument_type is missing")

    # ------------------------------------------------------------------
    # Check 8: notional_amount is null/empty
    # ------------------------------------------------------------------
    blank_notional = _is_blank(df["notional_amount"])
    for i in blank_notional[blank_notional].index:
        reasons[df.index.get_loc(i)].append("notional_amount is missing")

    # ------------------------------------------------------------------
    # Check 9: notional_amount not castable to float (only when non-blank)
    # ------------------------------------------------------------------
    non_numeric_notional = (~blank_notional) & (~_is_numeric(df["notional_amount"]))
    for i in non_numeric_notional[non_numeric_notional].index:
        reasons[df.index.get_loc(i)].append("notional_amount is not numeric")

    # ------------------------------------------------------------------
    # Check 10: currency is null/empty
    # ------------------------------------------------------------------
    blank_currency = _is_blank(df["currency"])
    for i in blank_currency[blank_currency].index:
        reasons[df.index.get_loc(i)].append("currency is missing")

    # ------------------------------------------------------------------
    # Check 11: currency not exactly 3 uppercase alpha chars (only when non-blank)
    # ------------------------------------------------------------------
    malformed_currency = (~blank_currency) & (~_is_valid_currency(df["currency"]))
    for i in malformed_currency[malformed_currency].index:
        reasons[df.index.get_loc(i)].append("currency is malformed")

    # ------------------------------------------------------------------
    # Check 12: counterparty_id is null/empty
    # ------------------------------------------------------------------
    blank_cpty = _is_blank(df["counterparty_id"])
    for i in blank_cpty[blank_cpty].index:
        reasons[df.index.get_loc(i)].append("counterparty_id is missing")

    # ------------------------------------------------------------------
    # LOGIC — split rows into valid and rejected
    # ------------------------------------------------------------------
    rejection_reason_series = pd.Series(
        ["; ".join(r) for r in reasons],
        index=df.index,
    )
    rejected_mask = rejection_reason_series.ne("")

    rejected_df = df[rejected_mask].copy()
    rejected_df["rejection_reason"] = rejection_reason_series[rejected_mask]

    valid_df = df[~rejected_mask].copy()

    # LOGIC — cast notional_amount to float64 on validated rows only
    if not valid_df.empty:
        valid_df["notional_amount"] = valid_df["notional_amount"].astype("float64")

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows.",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df