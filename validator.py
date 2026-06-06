# BOILERPLATE
import logging
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — all mandatory fields per data contract
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _check_mandatory_fields(row: pd.Series) -> list[str]:
    # LOGIC — a field fails if it is absent from the row, None/NaN, or empty string
    reasons: list[str] = []
    for field in _MANDATORY_FIELDS:
        value = row.get(field, None)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            reasons.append(f"Missing mandatory field: {field}")
        elif isinstance(value, str) and value.strip() == "":
            reasons.append(f"Missing mandatory field: {field}")
    return reasons


def _check_trade_date_consistency(
    row: pd.Series, expected_trade_date: str
) -> list[str]:
    # LOGIC — trade_date column value must match trade_date from the filename
    reasons: list[str] = []
    row_trade_date = row.get("trade_date", None)
    if row_trade_date is not None and not (
        isinstance(row_trade_date, float) and pd.isna(row_trade_date)
    ):
        if str(row_trade_date).strip() != expected_trade_date:
            reasons.append(
                f"trade_date mismatch: file states {expected_trade_date}, "
                f"row contains {row_trade_date}"
            )
    # LOGIC — if trade_date is null/empty it was already caught by mandatory-field check;
    # we do not double-report the consistency failure in that case
    return reasons


def _check_notional_amount(row: pd.Series) -> list[str]:
    # LOGIC — notional_amount must be castable to Decimal and must be >= 0
    reasons: list[str] = []
    raw_value = row.get("notional_amount", None)

    # LOGIC — null / empty already caught by mandatory-field check; skip here
    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return reasons
    if isinstance(raw_value, str) and raw_value.strip() == "":
        return reasons

    try:
        amount = Decimal(str(raw_value).strip())
    except InvalidOperation:
        reasons.append(
            f"notional_amount is not a valid non-negative number: {raw_value}"
        )
        return reasons

    if amount < Decimal("0"):
        reasons.append(
            f"notional_amount is not a valid non-negative number: {raw_value}"
        )
    return reasons


def validate_rows(
    df: pd.DataFrame, desk_code: str, trade_date: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — pre-compute duplicate trade_ids across the entire file so that
    # all occurrences (including the first) are flagged, not just the second+
    trade_id_col = df["trade_id"] if "trade_id" in df.columns else pd.Series([], dtype=object)

    # LOGIC — find trade_ids that appear more than once; .duplicated(keep=False)
    # marks ALL rows of a duplicated value as True
    if "trade_id" in df.columns:
        duplicate_mask = df["trade_id"].duplicated(keep=False)
        duplicate_trade_ids: set[str] = set(
            df.loc[duplicate_mask, "trade_id"].dropna().astype(str).tolist()
        )
    else:
        duplicate_trade_ids = set()

    valid_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for _, row in df.iterrows():
        all_reasons: list[str] = []

        # LOGIC — Rule 1: mandatory field presence
        all_reasons.extend(_check_mandatory_fields(row))

        # LOGIC — Rule 2: desk_code consistency (column vs filename)
        row_desk_code = row.get("desk_code", None)
        if row_desk_code is not None and not (
            isinstance(row_desk_code, float) and pd.isna(row_desk_code)
        ):
            if str(row_desk_code).strip() != desk_code:
                all_reasons.append(
                    f"desk_code mismatch: file states {desk_code}, "
                    f"row contains {row_desk_code}"
                )

        # LOGIC — Rule 3: trade_date consistency (column vs filename)
        all_reasons.extend(_check_trade_date_consistency(row, trade_date))

        # LOGIC — Rule 4: notional_amount format and non-negative check
        all_reasons.extend(_check_notional_amount(row))

        # LOGIC — Rule 5: duplicate trade_id within this file
        row_trade_id = row.get("trade_id", None)
        if row_trade_id is not None and not (
            isinstance(row_trade_id, float) and pd.isna(row_trade_id)
        ):
            if str(row_trade_id).strip() in duplicate_trade_ids:
                all_reasons.append(
                    f"Duplicate trade_id within file: {row_trade_id}"
                )

        row_dict = row.to_dict()

        if all_reasons:
            # LOGIC — rejected row: append all failure reasons as semicolon-delimited string
            row_dict["rejection_reason"] = "; ".join(all_reasons)
            rejected_rows.append(row_dict)
        else:
            valid_rows.append(row_dict)

    # BOILERPLATE — reconstruct DataFrames from collected rows
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows, columns=list(df.columns))
    else:
        valid_df = pd.DataFrame(columns=list(df.columns))

    if rejected_rows:
        rejected_columns = list(df.columns) + ["rejection_reason"]
        rejected_df = pd.DataFrame(rejected_rows, columns=rejected_columns)
    else:
        rejected_df = pd.DataFrame(columns=list(df.columns) + ["rejection_reason"])

    logger.info(
        "Validation complete: desk_code=%s trade_date=%s "
        "total=%d valid=%d rejected=%d",
        desk_code,
        trade_date,
        len(df),
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df