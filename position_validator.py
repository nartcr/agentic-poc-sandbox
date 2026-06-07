# BOILERPLATE
import logging
import re
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — mandatory fields required in every row
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — compiled date pattern for YYYY-MM-DD
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _check_mandatory_fields(df: pd.DataFrame) -> pd.Series:
    # LOGIC — returns a boolean Series: True where ALL mandatory fields are
    # non-null and non-empty-string.  The rejection_reason for the first
    # failing field is stored separately in validate_positions.
    if df.empty:
        return pd.Series(dtype=bool)

    valid_mask = pd.Series(True, index=df.index)
    for field in _MANDATORY_FIELDS:
        if field not in df.columns:
            # Column entirely absent — every row fails on this field
            valid_mask = pd.Series(False, index=df.index)
            break
        col_null = df[field].isna() | (df[field].astype(str).str.strip() == "")
        valid_mask = valid_mask & ~col_null

    return valid_mask


def _check_date_format(df: pd.DataFrame) -> pd.Series:
    # LOGIC — returns True where trade_date matches YYYY-MM-DD exactly.
    # Rows already rejected upstream are not present in df at call time.
    if df.empty:
        return pd.Series(dtype=bool)

    return df["trade_date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")


def _check_notional(df: pd.DataFrame) -> pd.Series:
    # LOGIC — returns True where notional_amount is castable to float AND > 0.
    # Rows already rejected upstream are not present in df at call time.
    if df.empty:
        return pd.Series(dtype=bool)

    valid_mask = pd.Series(True, index=df.index)

    def _is_numeric(val: str) -> bool:
        # LOGIC — attempt float conversion
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

    numeric_mask = df["notional_amount"].astype(str).apply(_is_numeric)
    valid_mask = valid_mask & numeric_mask

    # LOGIC — only check positivity where numeric conversion succeeds
    positive_mask = pd.Series(False, index=df.index)
    positive_mask[numeric_mask] = (
        df.loc[numeric_mask, "notional_amount"]
        .astype(str)
        .apply(lambda v: float(v) > 0)
    )
    valid_mask = valid_mask & (positive_mask | ~numeric_mask)
    # Combine: numeric=False already captured; positivity check only on numeric
    # Re-express cleanly:
    valid_mask = numeric_mask & positive_mask

    return valid_mask


def _check_intrafile_duplicates(df: pd.DataFrame) -> pd.Series:
    # LOGIC — within (trade_id, desk_code, trade_date), keep first occurrence;
    # all subsequent occurrences are marked as duplicates (return False).
    if df.empty:
        return pd.Series(dtype=bool)

    dup_mask = df.duplicated(subset=["trade_id", "desk_code", "trade_date"], keep="first")
    return ~dup_mask


def validate_positions(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — orchestrates all four validation rules in order.
    # Once a row is rejected it is excluded from subsequent checks.
    logger.info(
        "Starting validation on %d rows with columns: %s",
        len(raw_df),
        list(raw_df.columns),
    )

    if raw_df.empty:
        logger.warning("Received empty DataFrame — returning empty valid and rejected sets.")
        rejected_empty = raw_df.copy()
        rejected_empty["rejection_reason"] = pd.Series(dtype=str)
        rejected_empty["source_row_number"] = pd.Series(dtype=int)
        return raw_df.copy(), rejected_empty

    # LOGIC — assign 1-based source_row_number before any filtering
    working_df = raw_df.copy()
    working_df["source_row_number"] = range(1, len(working_df) + 1)

    # Accumulate rejected rows as a list of DataFrames
    rejected_frames: list[pd.DataFrame] = []

    # Remaining rows eligible for next rule
    remaining_df = working_df.copy()

    # ------------------------------------------------------------------ #
    # Rule 1: Mandatory field presence
    # ------------------------------------------------------------------ #
    # LOGIC — evaluate per-field so we can record which field failed first
    if not remaining_df.empty:
        rule1_valid_mask = pd.Series(True, index=remaining_df.index)
        first_failing_field: dict[int, str] = {}  # index → field name

        for field in _MANDATORY_FIELDS:
            if field not in remaining_df.columns:
                for idx in remaining_df.index:
                    if idx not in first_failing_field:
                        first_failing_field[idx] = field
                rule1_valid_mask.loc[remaining_df.index] = False
                break
            col_null = remaining_df[field].isna() | (
                remaining_df[field].astype(str).str.strip() == ""
            )
            for idx in remaining_df.index[col_null & rule1_valid_mask]:
                if idx not in first_failing_field:
                    first_failing_field[idx] = field
            rule1_valid_mask = rule1_valid_mask & ~col_null

        rule1_rejected = remaining_df[~rule1_valid_mask].copy()
        rule1_rejected["rejection_reason"] = rule1_rejected.index.map(
            lambda idx: f"MISSING_FIELD:{first_failing_field[idx]}"
        )
        if not rule1_rejected.empty:
            logger.info("Rule 1 (mandatory fields) rejected %d rows.", len(rule1_rejected))
            rejected_frames.append(rule1_rejected)

        remaining_df = remaining_df[rule1_valid_mask].copy()

    # ------------------------------------------------------------------ #
    # Rule 2: trade_date format
    # ------------------------------------------------------------------ #
    if not remaining_df.empty:
        rule2_valid_mask = _check_date_format(remaining_df)
        rule2_rejected = remaining_df[~rule2_valid_mask].copy()
        rule2_rejected["rejection_reason"] = "INVALID_DATE_FORMAT:trade_date"
        if not rule2_rejected.empty:
            logger.info("Rule 2 (date format) rejected %d rows.", len(rule2_rejected))
            rejected_frames.append(rule2_rejected)

        remaining_df = remaining_df[rule2_valid_mask].copy()

    # ------------------------------------------------------------------ #
    # Rule 3: notional_amount numeric and positive
    # ------------------------------------------------------------------ #
    if not remaining_df.empty:
        # LOGIC — distinguish not_numeric from non_positive for correct reason code
        def _notional_reason(val: str) -> str:
            # LOGIC — returns rejection reason or empty string if valid
            try:
                f = float(val)
            except (ValueError, TypeError):
                return "INVALID_NOTIONAL:not_numeric"
            if f <= 0:
                return "INVALID_NOTIONAL:non_positive"
            return ""

        notional_reasons = remaining_df["notional_amount"].astype(str).apply(_notional_reason)
        rule3_valid_mask = notional_reasons == ""
        rule3_rejected = remaining_df[~rule3_valid_mask].copy()
        rule3_rejected["rejection_reason"] = notional_reasons[~rule3_valid_mask]
        if not rule3_rejected.empty:
            logger.info("Rule 3 (notional) rejected %d rows.", len(rule3_rejected))
            rejected_frames.append(rule3_rejected)

        remaining_df = remaining_df[rule3_valid_mask].copy()

    # ------------------------------------------------------------------ #
    # Rule 4: Intra-file duplicates
    # ------------------------------------------------------------------ #
    if not remaining_df.empty:
        rule4_valid_mask = _check_intrafile_duplicates(remaining_df)
        rule4_rejected = remaining_df[~rule4_valid_mask].copy()
        rule4_rejected["rejection_reason"] = "DUPLICATE_IN_FILE"
        if not rule4_rejected.empty:
            logger.info("Rule 4 (intra-file duplicates) rejected %d rows.", len(rule4_rejected))
            rejected_frames.append(rule4_rejected)

        remaining_df = remaining_df[rule4_valid_mask].copy()

    # ------------------------------------------------------------------ #
    # Assemble final valid and rejected DataFrames
    # ------------------------------------------------------------------ #
    # LOGIC — valid_df: drop the helper columns before returning
    valid_df = remaining_df.drop(columns=["source_row_number"]).copy()

    if rejected_frames:
        rejected_df = pd.concat(rejected_frames, axis=0).sort_values(
            "source_row_number"
        ).reset_index(drop=True)
        # LOGIC — ensure rejection_reason and source_row_number are present
        # but do NOT include source_row_number in valid_df
    else:
        rejected_df = working_df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        # source_row_number column is already in working_df schema

    # LOGIC — remove source_row_number from valid_df (it was a working column)
    if "source_row_number" in valid_df.columns:
        valid_df = valid_df.drop(columns=["source_row_number"])

    logger.info(
        "Validation complete — valid: %d, rejected: %d.",
        len(valid_df),
        len(rejected_df),
    )
    return valid_df, rejected_df