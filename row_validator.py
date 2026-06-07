# BOILERPLATE
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — ordered validation check labels
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _check_missing_fields(row: pd.Series) -> list[str]:
    # LOGIC — rule 1: all required fields must be non-null and non-empty string
    reasons = []
    for col in _REQUIRED_COLUMNS:
        val = row.get(col)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            reasons.append(f"missing_field:{col}")
        elif isinstance(val, str) and val.strip() == "":
            reasons.append(f"missing_field:{col}")
    return reasons


def _check_trade_date(val: str) -> list[str]:
    # LOGIC — rule 2: trade_date must parse as YYYY-MM-DD
    try:
        datetime.strptime(val.strip(), "%Y-%m-%d")
        return []
    except (ValueError, AttributeError):
        return ["invalid_date"]


def _check_notional_amount(val: str) -> list[str]:
    # LOGIC — rule 3: notional_amount must be castable to Decimal
    try:
        Decimal(str(val).strip())
        return []
    except (InvalidOperation, ValueError):
        return ["invalid_notional_amount"]


def _check_currency_length(val: str) -> list[str]:
    # LOGIC — rule 4: currency must be exactly 3 characters
    if isinstance(val, str) and len(val.strip()) == 3:
        return []
    return ["invalid_currency_length"]


def _build_rejection_reason(reasons: list[str]) -> str:
    # LOGIC — pipe-delimit all failure labels; first is primary
    return "|".join(reasons)


def validate_rows(
    df: pd.DataFrame,
    desk_code: str,
    trade_date_str: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — main validation entry point; returns (valid_df, rejected_df)
    if df.empty:
        logger.warning("Input DataFrame is empty; returning empty valid and rejected sets.")
        valid_df = pd.DataFrame(columns=_REQUIRED_COLUMNS)
        rejected_df = pd.DataFrame(columns=_REQUIRED_COLUMNS + ["rejection_reason"])
        return valid_df, rejected_df

    # LOGIC — ensure all required columns exist; if missing, every row is rejected
    for col in _REQUIRED_COLUMNS:
        if col not in df.columns:
            logger.warning("Required column '%s' missing from input DataFrame; all rows rejected.", col)
            df = df.copy()
            df[col] = None

    valid_rows = []
    rejected_rows = []

    # LOGIC — track seen (trade_id, desk_code, trade_date) tuples for duplicate detection
    seen_keys: set[tuple] = set()

    for idx, row in df.iterrows():
        reasons: list[str] = []

        # LOGIC — rule 1: missing/null check first
        missing_reasons = _check_missing_fields(row)
        reasons.extend(missing_reasons)

        # LOGIC — only run further checks if the field exists and is non-empty
        missing_fields = {r.split(":")[1] for r in missing_reasons}

        # LOGIC — rule 2: trade_date format (only if trade_date present)
        if "trade_date" not in missing_fields:
            reasons.extend(_check_trade_date(str(row["trade_date"])))

        # LOGIC — rule 3: notional_amount numeric (only if notional_amount present)
        if "notional_amount" not in missing_fields:
            reasons.extend(_check_notional_amount(str(row["notional_amount"])))

        # LOGIC — rule 4: currency length (only if currency present)
        if "currency" not in missing_fields:
            reasons.extend(_check_currency_length(str(row["currency"])))

        if reasons:
            # LOGIC — row already failed at least one check; record and skip duplicate check
            rejected_row = row.to_dict()
            rejected_row["rejection_reason"] = _build_rejection_reason(reasons)
            rejected_rows.append(rejected_row)
            logger.debug("Row %s rejected: %s", idx, rejected_row["rejection_reason"])
            continue

        # LOGIC — rule 5: duplicate (trade_id, desk_code, trade_date) within file
        row_trade_id = str(row["trade_id"]).strip()
        row_desk_code = str(row["desk_code"]).strip()
        row_trade_date = str(row["trade_date"]).strip()
        dedup_key = (row_trade_id, row_desk_code, row_trade_date)

        if dedup_key in seen_keys:
            rejected_row = row.to_dict()
            rejected_row["rejection_reason"] = "duplicate_within_file"
            rejected_rows.append(rejected_row)
            logger.debug("Row %s rejected as duplicate: %s", idx, dedup_key)
            continue

        seen_keys.add(dedup_key)

        # LOGIC — row passed all checks; coerce types for downstream use
        coerced = row.to_dict()
        try:
            coerced["trade_date"] = datetime.strptime(row_trade_date, "%Y-%m-%d").date()
        except ValueError:
            # LOGIC — should not occur given rule 2 passed, but guard defensively
            rejected_row = coerced.copy()
            rejected_row["rejection_reason"] = "invalid_date"
            rejected_rows.append(rejected_row)
            logger.error("Unexpected date coercion failure for row %s.", idx)
            continue

        try:
            coerced["notional_amount"] = Decimal(str(row["notional_amount"]).strip())
        except (InvalidOperation, ValueError):
            # LOGIC — should not occur given rule 3 passed, but guard defensively
            rejected_row = coerced.copy()
            rejected_row["rejection_reason"] = "invalid_notional_amount"
            rejected_rows.append(rejected_row)
            logger.error("Unexpected notional coercion failure for row %s.", idx)
            continue

        valid_rows.append(coerced)

    # LOGIC — assemble output DataFrames with correct column sets
    if valid_rows:
        valid_df = pd.DataFrame(valid_rows)[_REQUIRED_COLUMNS]
    else:
        valid_df = pd.DataFrame(columns=_REQUIRED_COLUMNS)

    rejected_columns = _REQUIRED_COLUMNS + ["rejection_reason"]
    if rejected_rows:
        rejected_df = pd.DataFrame(rejected_rows)
        # LOGIC — ensure rejection_reason column is present even if extra columns exist
        for col in rejected_columns:
            if col not in rejected_df.columns:
                rejected_df[col] = None
    else:
        rejected_df = pd.DataFrame(columns=rejected_columns)

    logger.info(
        "Validation complete: %d valid, %d rejected (desk_code=%s, trade_date=%s).",
        len(valid_df),
        len(rejected_df),
        desk_code,
        trade_date_str,
    )

    return valid_df, rejected_df