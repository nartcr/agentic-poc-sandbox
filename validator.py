# BOILERPLATE
import logging
import re
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — validation constants
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "trade_date",
]

# LOGIC — ISO 4217 pattern: exactly 3 uppercase alphabetic characters
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# LOGIC — trade_date format: YYYY-MM-DD
_TRADE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_rows(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — work on a copy to avoid mutating the caller's DataFrame
    working = df.copy()

    # LOGIC — assign 1-based source_row_number before any filtering so it
    # always reflects the original file row order
    working = working.reset_index(drop=True)
    working["source_row_number"] = working.index + 1

    # LOGIC — rejection reason accumulator: one list of strings per row
    num_rows = len(working)
    rejection_reasons: list = [[] for _ in range(num_rows)]

    # -----------------------------------------------------------------------
    # Rule 1: Mandatory field presence
    # -----------------------------------------------------------------------
    for field in _MANDATORY_FIELDS:
        if field not in working.columns:
            # LOGIC — entire column missing: every row fails this check
            for i in range(num_rows):
                rejection_reasons[i].append(f"Missing mandatory field: {field}")
        else:
            # LOGIC — null or empty-string values are both considered missing
            missing_mask = working[field].isnull() | (working[field].astype(str).str.strip() == "")
            for i in working.index[missing_mask]:
                rejection_reasons[i].append(f"Missing mandatory field: {field}")

    # -----------------------------------------------------------------------
    # Rule 2: trade_date format and filename match
    # -----------------------------------------------------------------------
    if "trade_date" in working.columns:
        # LOGIC — first check format, then check against filename trade_date
        td_series = working["trade_date"].fillna("").astype(str)
        invalid_format_mask = ~td_series.str.match(_TRADE_DATE_RE)
        mismatch_mask = td_series != trade_date
        # LOGIC — combine: bad format OR value doesn't match filename date
        rule2_mask = invalid_format_mask | mismatch_mask
        # LOGIC — but only flag rows that were not already flagged for missing
        # (if trade_date is empty it already has a missing-field reason; we
        # still add this reason for non-empty but wrong/mismatched values)
        for i in working.index[rule2_mask]:
            value = td_series.iloc[i]
            rejection_reasons[i].append(
                f"Invalid trade_date format or mismatch with filename: {value}"
            )

    # -----------------------------------------------------------------------
    # Rule 3: desk_code consistency with filename
    # -----------------------------------------------------------------------
    if "desk_code" in working.columns:
        dc_series = working["desk_code"].fillna("").astype(str)
        rule3_mask = dc_series != desk_code
        for i in working.index[rule3_mask]:
            value = dc_series.iloc[i]
            rejection_reasons[i].append(
                f"desk_code mismatch with filename: {value}"
            )

    # -----------------------------------------------------------------------
    # Rule 4: notional_amount numeric and finite
    # -----------------------------------------------------------------------
    if "notional_amount" in working.columns:
        na_series = working["notional_amount"].fillna("").astype(str)
        # LOGIC — coerce to numeric; non-parseable values become NaN
        numeric_series = pd.to_numeric(na_series, errors="coerce")
        # LOGIC — NaN (parse failure) or non-finite (inf/-inf) are both invalid
        rule4_mask = ~np.isfinite(numeric_series.values)
        for i in working.index[rule4_mask]:
            value = working["notional_amount"].iloc[i]
            rejection_reasons[i].append(
                f"notional_amount is not a valid number: {value}"
            )

    # -----------------------------------------------------------------------
    # Rule 5: currency 3-letter uppercase alphabetic (ISO 4217 pattern)
    # -----------------------------------------------------------------------
    if "currency" in working.columns:
        curr_series = working["currency"].fillna("").astype(str)
        rule5_mask = ~curr_series.str.match(_CURRENCY_RE)
        for i in working.index[rule5_mask]:
            value = working["currency"].iloc[i]
            rejection_reasons[i].append(
                f"currency is not a valid 3-letter code: {value}"
            )

    # -----------------------------------------------------------------------
    # LOGIC — split into valid and rejected DataFrames
    # -----------------------------------------------------------------------
    has_rejection = [len(reasons) > 0 for reasons in rejection_reasons]

    rejected_mask = pd.Series(has_rejection, index=working.index)
    valid_mask = ~rejected_mask

    # LOGIC — build rejected_df with all original columns + rejection_reason + source_row_number
    rejected_df = working[rejected_mask].copy()
    rejected_df["rejection_reason"] = [
        "; ".join(reasons)
        for reasons, is_rejected in zip(rejection_reasons, has_rejection)
        if is_rejected
    ]

    # LOGIC — build valid_df; cast notional_amount to float64
    valid_df = working[valid_mask].copy()
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        valid_df["notional_amount"] = pd.to_numeric(
            valid_df["notional_amount"], errors="coerce"
        ).astype("float64")

    logger.info(
        "validate_rows: total=%d valid=%d rejected=%d (desk_code=%s, trade_date=%s)",
        num_rows,
        len(valid_df),
        len(rejected_df),
        desk_code,
        trade_date,
    )

    if not rejected_df.empty:
        logger.info(
            "Rejection reason breakdown for desk_code=%s trade_date=%s:\n%s",
            desk_code,
            trade_date,
            rejected_df[["source_row_number", "rejection_reason"]].to_string(index=False),
        )

    return valid_df, rejected_df