# BOILERPLATE
import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)

# BOILERPLATE — mandatory column set as defined in data contract
MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — ISO 4217: exactly 3 alphabetic characters
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")


def _check_mandatory_fields(row: pd.Series) -> list:
    # LOGIC — presence check: non-null, non-empty after strip
    failures = []
    for field in MANDATORY_FIELDS:
        value = row.get(field)
        if value is None or str(value).strip() == "":
            failures.append(f"{field} is missing or empty")
    return failures


def _check_trade_date_format(value: str) -> list:
    # LOGIC — must parse as YYYY-MM-DD
    failures = []
    try:
        datetime.strptime(value.strip(), "%Y-%m-%d")
    except (ValueError, AttributeError):
        failures.append("trade_date format invalid: expected YYYY-MM-DD")
    return failures


def _check_notional_amount(value: str) -> list:
    # LOGIC — must be castable to Decimal with no exception
    failures = []
    try:
        Decimal(value.strip())
    except (InvalidOperation, ValueError, AttributeError):
        failures.append("notional_amount is not numeric")
    return failures


def _check_currency_format(value: str) -> list:
    # LOGIC — exactly 3 alphabetic characters (ISO 4217)
    failures = []
    if not _CURRENCY_RE.match(value.strip() if value else ""):
        failures.append("currency must be 3 alphabetic characters")
    return failures


def _check_intrafile_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    # LOGIC — flag rows where trade_id appears more than once for same desk_code + trade_date
    # Only called on the subset of rows that passed all per-row checks.
    duplicate_mask = df.duplicated(
        subset=["trade_id", "desk_code", "trade_date"], keep=False
    )
    if duplicate_mask.any():
        dup_count = duplicate_mask.sum()
        logger.warning(
            "Intra-file duplicate check found %d rows with duplicate "
            "(trade_id, desk_code, trade_date) combinations",
            dup_count,
        )
    result = df.copy()
    result.loc[duplicate_mask, "rejection_reason"] = (
        "duplicate trade_id within file for same desk_code and trade_date"
    )
    return result


def validate_rows(df: pd.DataFrame) -> tuple:
    # LOGIC — orchestrates all validation rules in order, collects all failures per row
    """
    Returns (valid_df, rejected_df).

    valid_df columns:
        trade_id (str), desk_code (str), trade_date (date),
        instrument_type (str), notional_amount (Decimal),
        currency (str), counterparty_id (str)

    rejected_df columns:
        all original columns + rejection_reason (str)
    """
    if df.empty:
        logger.info("validate_rows received an empty DataFrame; returning empty results")
        valid_df = pd.DataFrame(columns=MANDATORY_FIELDS)
        rejected_df = pd.DataFrame(columns=list(df.columns) + ["rejection_reason"])
        return valid_df, rejected_df

    # LOGIC — work on a copy; preserve original index for row traceability
    working = df.copy()
    rejection_reasons: list = [None] * len(working)

    for idx, (pos, row) in enumerate(working.iterrows()):
        row_failures: list = []

        # Rule 1: presence check
        presence_failures = _check_mandatory_fields(row)
        row_failures.extend(presence_failures)

        # Rule 2: trade_date format (only if field is present)
        trade_date_val = row.get("trade_date")
        if trade_date_val is not None and str(trade_date_val).strip() != "":
            row_failures.extend(_check_trade_date_format(str(trade_date_val)))

        # Rule 3: notional_amount numeric (only if field is present)
        notional_val = row.get("notional_amount")
        if notional_val is not None and str(notional_val).strip() != "":
            row_failures.extend(_check_notional_amount(str(notional_val)))

        # Rule 4: currency format (only if field is present)
        currency_val = row.get("currency")
        if currency_val is not None and str(currency_val).strip() != "":
            row_failures.extend(_check_currency_format(str(currency_val)))

        if row_failures:
            rejection_reasons[idx] = "; ".join(row_failures)

    # LOGIC — split into passing and failing sets based on per-row checks
    passing_mask = [reason is None for reason in rejection_reasons]
    failing_mask = [reason is not None for reason in rejection_reasons]

    passing_df = working[passing_mask].copy()
    failing_df = working[failing_mask].copy()
    failing_df["rejection_reason"] = [r for r in rejection_reasons if r is not None]

    logger.info(
        "Per-row validation complete: %d passing, %d failing",
        len(passing_df),
        len(failing_df),
    )

    # Rule 5: intra-file duplicate detection on the passing set
    if not passing_df.empty:
        passing_df_with_dup_check = _check_intrafile_duplicates(passing_df)

        # LOGIC — rows flagged as duplicates are moved to the rejected set
        dup_flagged_mask = passing_df_with_dup_check["rejection_reason"].notna()
        newly_rejected = passing_df_with_dup_check[dup_flagged_mask].copy()
        final_passing = passing_df_with_dup_check[~dup_flagged_mask].copy()

        if "rejection_reason" in final_passing.columns:
            final_passing = final_passing.drop(columns=["rejection_reason"])

        if not newly_rejected.empty:
            logger.warning(
                "%d rows rejected due to intra-file duplicate trade_id",
                len(newly_rejected),
            )
            failing_df = pd.concat([failing_df, newly_rejected], ignore_index=True)
    else:
        final_passing = passing_df

    logger.info(
        "After duplicate check: %d valid rows, %d total rejected rows",
        len(final_passing),
        len(failing_df),
    )

    # LOGIC — type-cast valid rows: trade_date → date, notional_amount → Decimal
    if not final_passing.empty:
        final_passing = _cast_valid_row_types(final_passing)

    return final_passing, failing_df


def _cast_valid_row_types(df: pd.DataFrame) -> pd.DataFrame:
    # LOGIC — convert string columns to target types for valid rows only
    result = df.copy()

    result["trade_date"] = result["trade_date"].apply(
        lambda v: datetime.strptime(str(v).strip(), "%Y-%m-%d").date()
    )
    result["notional_amount"] = result["notional_amount"].apply(
        lambda v: Decimal(str(v).strip())
    )

    # LOGIC — ensure string columns are stripped of surrounding whitespace
    for col in ["trade_id", "desk_code", "instrument_type", "currency", "counterparty_id"]:
        result[col] = result[col].apply(lambda v: str(v).strip())

    logger.debug("Type casting complete for %d valid rows", len(result))
    return result