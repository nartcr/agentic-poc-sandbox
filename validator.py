# BOILERPLATE
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — the 7 required columns, in canonical order
REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — required columns excluding notional_amount (string-presence checks)
_STRING_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "currency",
    "counterparty_id",
]

# LOGIC — YYYY-MM-DD date pattern
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_numeric(value: str) -> bool:
    """
    # LOGIC
    Return True if the string value can be converted to a float.
    Handles leading/trailing whitespace.
    """
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def validate(
    df: pd.DataFrame,
    desk_code_from_filename: str,
    trade_date_from_filename: str,
) -> tuple:
    """
    # LOGIC
    Validate each row of the raw DataFrame against 7 ordered business rules.

    Returns:
        (valid_df, rejected_df)
        - valid_df: rows passing all rules; notional_amount cast to float64,
          trade_date cast to datetime.date
        - rejected_df: rows failing at least one rule; all original columns
          plus rejection_reason (str); reason is from the FIRST failing rule
    """
    logger.info(
        "Starting validation: %d rows, desk_code_from_filename='%s', "
        "trade_date_from_filename='%s'",
        len(df),
        desk_code_from_filename,
        trade_date_from_filename,
    )

    # LOGIC — Rule 1: missing required columns check
    # If any required column is absent from the DataFrame entirely, ALL rows
    # are rejected with reason "missing_column:{column_name}" for the first
    # missing column found.
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        first_missing = missing_columns[0]
        rejection_reason = f"missing_column:{first_missing}"
        logger.warning(
            "Required column(s) missing from DataFrame: %s. "
            "All %d rows rejected with reason '%s'.",
            missing_columns,
            len(df),
            rejection_reason,
        )
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = rejection_reason
        valid_df = df.iloc[0:0].copy()  # empty DataFrame preserving columns
        return valid_df, rejected_df

    # LOGIC — work with a copy to avoid mutating caller's DataFrame
    work_df = df.copy()
    n = len(work_df)

    # LOGIC — track which rows have already been rejected (boolean mask)
    already_rejected = pd.Series([False] * n, index=work_df.index)
    # LOGIC — store rejection reason per row; empty string means not yet rejected
    rejection_reasons = pd.Series([""] * n, index=work_df.index, dtype=object)

    # ------------------------------------------------------------------
    # Rule 2: Null/empty field check for string-required columns
    # ------------------------------------------------------------------
    # LOGIC — iterate columns in defined order so the first null field
    # encountered per row is recorded as the rejection reason
    for col in _STRING_REQUIRED_COLUMNS:
        # rows not yet rejected, where this column is null or empty string
        candidate_mask = ~already_rejected
        null_mask = work_df[col].isnull()
        # treat empty string and whitespace-only as empty
        empty_mask = (~null_mask) & work_df[col].str.strip().eq("")
        failing = candidate_mask & (null_mask | empty_mask)
        reason = f"null_required_field:{col}"
        rejection_reasons = rejection_reasons.where(~failing, other=reason)
        already_rejected = already_rejected | failing
        if failing.any():
            logger.debug(
                "Rule 2 (%s): %d rows rejected with '%s'",
                col,
                int(failing.sum()),
                reason,
            )

    # ------------------------------------------------------------------
    # Rule 3: Null notional check
    # ------------------------------------------------------------------
    # LOGIC — null notional_amount (not yet rejected)
    candidate_mask = ~already_rejected
    null_notional = work_df["notional_amount"].isnull()
    failing = candidate_mask & null_notional
    if failing.any():
        reason = "null_required_field:notional_amount"
        rejection_reasons = rejection_reasons.where(~failing, other=reason)
        already_rejected = already_rejected | failing
        logger.debug(
            "Rule 3 (null notional): %d rows rejected", int(failing.sum())
        )

    # ------------------------------------------------------------------
    # Rule 4: notional_amount numeric check
    # ------------------------------------------------------------------
    # LOGIC — rows not yet rejected where notional_amount is non-null but
    # cannot be cast to float
    candidate_mask = ~already_rejected
    non_null_notional = ~work_df["notional_amount"].isnull()
    numeric_check_candidates = candidate_mask & non_null_notional

    if numeric_check_candidates.any():
        non_numeric_mask = ~work_df.loc[numeric_check_candidates, "notional_amount"].apply(
            _is_numeric
        )
        # re-index back to full index
        non_numeric_full = pd.Series(False, index=work_df.index)
        non_numeric_full.loc[non_numeric_mask[non_numeric_mask].index] = True
        failing = numeric_check_candidates & non_numeric_full
        if failing.any():
            reason = "invalid_numeric:notional_amount"
            rejection_reasons = rejection_reasons.where(~failing, other=reason)
            already_rejected = already_rejected | failing
            logger.debug(
                "Rule 4 (non-numeric notional): %d rows rejected",
                int(failing.sum()),
            )

    # ------------------------------------------------------------------
    # Rule 5: trade_date format check
    # ------------------------------------------------------------------
    # LOGIC — rows not yet rejected where trade_date does not match YYYY-MM-DD
    candidate_mask = ~already_rejected
    # Only apply to rows where trade_date is non-null and non-empty
    # (null/empty already caught by Rule 2 if applicable)
    if candidate_mask.any():
        date_values = work_df.loc[candidate_mask, "trade_date"].fillna("")
        bad_date_mask = ~date_values.str.strip().apply(
            lambda v: bool(_DATE_PATTERN.match(v)) if v else False
        )
        bad_date_full = pd.Series(False, index=work_df.index)
        bad_date_full.loc[bad_date_mask[bad_date_mask].index] = True
        failing = candidate_mask & bad_date_full
        if failing.any():
            reason = "invalid_date_format:trade_date"
            rejection_reasons = rejection_reasons.where(~failing, other=reason)
            already_rejected = already_rejected | failing
            logger.debug(
                "Rule 5 (bad date format): %d rows rejected", int(failing.sum())
            )

    # ------------------------------------------------------------------
    # Rule 6: desk_code consistency check
    # ------------------------------------------------------------------
    # LOGIC — rows not yet rejected where CSV desk_code != filename desk_code
    candidate_mask = ~already_rejected
    if candidate_mask.any():
        mismatch_mask = work_df.loc[candidate_mask, "desk_code"].str.strip().ne(
            desk_code_from_filename
        )
        mismatch_full = pd.Series(False, index=work_df.index)
        mismatch_full.loc[mismatch_mask[mismatch_mask].index] = True
        failing = candidate_mask & mismatch_full
        if failing.any():
            reason = "desk_code_mismatch"
            rejection_reasons = rejection_reasons.where(~failing, other=reason)
            already_rejected = already_rejected | failing
            logger.debug(
                "Rule 6 (desk_code mismatch): %d rows rejected",
                int(failing.sum()),
            )

    # ------------------------------------------------------------------
    # Rule 7: trade_date consistency check
    # ------------------------------------------------------------------
    # LOGIC — rows not yet rejected where CSV trade_date != filename trade_date
    candidate_mask = ~already_rejected
    if candidate_mask.any():
        mismatch_mask = work_df.loc[candidate_mask, "trade_date"].str.strip().ne(
            trade_date_from_filename
        )
        mismatch_full = pd.Series(False, index=work_df.index)
        mismatch_full.loc[mismatch_mask[mismatch_mask].index] = True
        failing = candidate_mask & mismatch_full
        if failing.any():
            reason = "trade_date_mismatch"
            rejection_reasons = rejection_reasons.where(~failing, other=reason)
            already_rejected = already_rejected | failing
            logger.debug(
                "Rule 7 (trade_date mismatch): %d rows rejected",
                int(failing.sum()),
            )

    # ------------------------------------------------------------------
    # LOGIC — split into valid and rejected subsets
    # ------------------------------------------------------------------
    valid_mask = ~already_rejected
    valid_rows = work_df[valid_mask].copy()
    rejected_rows = work_df[already_rejected].copy()
    rejected_rows["rejection_reason"] = rejection_reasons[already_rejected]

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        int(valid_mask.sum()),
        int(already_rejected.sum()),
    )

    # ------------------------------------------------------------------
    # LOGIC — type casts on valid rows only
    # notional_amount -> float64
    # trade_date -> datetime.date (via pd.to_datetime then .dt.date)
    # ------------------------------------------------------------------
    if not valid_rows.empty:
        valid_rows["notional_amount"] = valid_rows["notional_amount"].astype(
            "float64"
        )
        valid_rows["trade_date"] = pd.to_datetime(
            valid_rows["trade_date"].str.strip(), format="%Y-%m-%d"
        ).dt.date
        logger.debug(
            "Type casts applied to %d valid rows: "
            "notional_amount -> float64, trade_date -> date",
            len(valid_rows),
        )

    return valid_rows, rejected_rows