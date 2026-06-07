# BOILERPLATE
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — mandatory columns required by the data contract
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_missing(value) -> bool:
    # LOGIC — treat NaN, None, and empty/whitespace string as missing
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _check_presence(row: pd.Series) -> str | None:
    # LOGIC — presence check: all seven mandatory fields must be non-null, non-empty
    for col in MANDATORY_COLUMNS:
        if col not in row.index or _is_missing(row[col]):
            return f"missing: {col}"
    return None


def _check_notional_type(row: pd.Series) -> str | None:
    # LOGIC — notional_amount must parse as a positive float
    value = row["notional_amount"]
    try:
        parsed = float(value)
        if parsed <= 0:
            return "invalid_type: notional_amount not numeric"
    except (ValueError, TypeError):
        return "invalid_type: notional_amount not numeric"
    return None


def _check_trade_date_format(row: pd.Series) -> str | None:
    # LOGIC — trade_date must be parseable as YYYY-MM-DD
    value = row["trade_date"]
    try:
        datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except (ValueError, TypeError):
        return "invalid_date: trade_date format not YYYY-MM-DD"
    return None


def _check_desk_code_consistency(row: pd.Series, filename_desk_code: str) -> str | None:
    # LOGIC — desk_code in each row must match the desk_code from the filename
    row_desk_code = str(row["desk_code"]).strip()
    if row_desk_code != filename_desk_code.strip():
        return "desk_code_mismatch: row desk_code does not match filename"
    return None


def _validate_single_row(row: pd.Series, filename_desk_code: str) -> str | None:
    # LOGIC — apply rules in priority order; return first failing reason
    reason = _check_presence(row)
    if reason is not None:
        return reason

    reason = _check_notional_type(row)
    if reason is not None:
        return reason

    reason = _check_trade_date_format(row)
    if reason is not None:
        return reason

    reason = _check_desk_code_consistency(row, filename_desk_code)
    if reason is not None:
        return reason

    return None


def validate_rows(
    raw_df: pd.DataFrame,
    filename_desk_code: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — entry point: splits raw_df into valid_df and rejected_df
    if raw_df.empty:
        logger.warning("validate_rows received an empty DataFrame")
        empty_rejected = raw_df.copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return raw_df.copy(), empty_rejected

    logger.info(
        "Starting row validation: total_rows=%d filename_desk_code=%s",
        len(raw_df),
        filename_desk_code,
    )

    # LOGIC — apply per-row validation; collect rejection reasons
    rejection_reasons: list[str | None] = []
    for _, row in raw_df.iterrows():
        reason = _validate_single_row(row, filename_desk_code)
        rejection_reasons.append(reason)

    # LOGIC — build boolean mask
    is_rejected = [r is not None for r in rejection_reasons]
    is_valid = [r is None for r in rejection_reasons]

    valid_df = raw_df.loc[is_valid].copy()
    rejected_df = raw_df.loc[is_rejected].copy()

    # LOGIC — attach rejection_reason column to rejected rows only
    rejected_reasons_series = [
        reason for reason in rejection_reasons if reason is not None
    ]
    rejected_df = rejected_df.reset_index(drop=True)
    rejected_df["rejection_reason"] = rejected_reasons_series

    valid_df = valid_df.reset_index(drop=True)

    logger.info(
        "Row validation complete: valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df