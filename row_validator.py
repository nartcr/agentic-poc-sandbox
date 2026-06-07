# BOILERPLATE
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — Regex patterns for validation rules
_TRADE_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")

# LOGIC — All columns that must be present in the output rejected DataFrame
_ALL_INPUT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_null_or_empty(value) -> bool:
    # LOGIC — Detect pandas NaN, Python None, or empty/whitespace string
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _check_mandatory_fields(row: pd.Series) -> str | None:
    # LOGIC — Check all mandatory fields for null/empty; accumulate all failures
    failures = []

    if _is_null_or_empty(row.get("trade_id")):
        failures.append("trade_id: missing")

    if _is_null_or_empty(row.get("desk_code")):
        failures.append("desk_code: missing")

    if _is_null_or_empty(row.get("trade_date")):
        failures.append("trade_date: missing")

    if _is_null_or_empty(row.get("instrument_type")):
        failures.append("instrument_type: missing")

    if _is_null_or_empty(row.get("notional_amount")):
        failures.append("notional_amount: missing")

    if _is_null_or_empty(row.get("currency")):
        failures.append("currency: missing")

    if _is_null_or_empty(row.get("counterparty_id")):
        failures.append("counterparty_id: missing")

    if failures:
        return "|".join(failures)
    return None


def _check_notional_amount(value) -> str | None:
    # LOGIC — Validate notional_amount is numeric and non-negative
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return "notional_amount: not numeric"

    if numeric_value < 0:
        return "notional_amount: negative value"

    return None


def _check_trade_date_format(value: str) -> str | None:
    # LOGIC — Validate trade_date matches YYYY-MM-DD pattern
    str_value = str(value).strip()
    if not _TRADE_DATE_PATTERN.match(str_value):
        return "trade_date: invalid format (expected YYYY-MM-DD)"
    return None


def _check_desk_code_match(value: str, expected: str) -> str | None:
    # LOGIC — Validate desk_code in row matches the desk_code parsed from filename
    str_value = str(value).strip()
    if str_value != expected:
        return f"desk_code: value '{str_value}' does not match expected '{expected}'"
    return None


def _validate_row(
    row: pd.Series, expected_desk_code: str, expected_trade_date: str
) -> str | None:
    # LOGIC — Apply all validation rules to a single row; return combined rejection reason or None
    failures = []

    # Rule 1: Check all mandatory fields for presence (null/empty)
    mandatory_failure = _check_mandatory_fields(row)
    if mandatory_failure:
        failures.append(mandatory_failure)

    # Rules 2–7: Field-level format/value checks (only when field is not null/empty)
    # Rule 2: desk_code must match expected value (only if not missing)
    if not _is_null_or_empty(row.get("desk_code")):
        desk_failure = _check_desk_code_match(str(row.get("desk_code")), expected_desk_code)
        if desk_failure:
            failures.append(desk_failure)

    # Rule 3: trade_date must match YYYY-MM-DD and must equal expected_trade_date
    if not _is_null_or_empty(row.get("trade_date")):
        date_format_failure = _check_trade_date_format(str(row.get("trade_date")))
        if date_format_failure:
            failures.append(date_format_failure)
        else:
            # LOGIC — Only check date value match if format is valid
            str_date = str(row.get("trade_date")).strip()
            if str_date != expected_trade_date:
                failures.append(
                    f"trade_date: value '{str_date}' does not match expected '{expected_trade_date}'"
                )

    # Rule 5: notional_amount must be numeric and >= 0 (only if not missing)
    if not _is_null_or_empty(row.get("notional_amount")):
        notional_failure = _check_notional_amount(row.get("notional_amount"))
        if notional_failure:
            failures.append(notional_failure)

    # Rule 6: currency must match [A-Z]{3} (only if not missing)
    if not _is_null_or_empty(row.get("currency")):
        currency_value = str(row.get("currency")).strip()
        if not _CURRENCY_PATTERN.fullmatch(currency_value):
            failures.append(
                f"currency: value '{currency_value}' does not match [A-Z]{{3}} pattern"
            )

    if failures:
        return "|".join(failures)
    return None


def validate_rows(
    df: pd.DataFrame,
    expected_desk_code: str,
    expected_trade_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # LOGIC — Split DataFrame into valid and rejected rows; attach rejection_reason to rejected rows
    logger.info(
        "Validating %d rows for desk_code='%s', trade_date='%s'",
        len(df),
        expected_desk_code,
        expected_trade_date,
    )

    valid_indices = []
    rejected_indices = []
    rejection_reasons = {}

    for idx, row in df.iterrows():
        reason = _validate_row(row, expected_desk_code, expected_trade_date)
        if reason is None:
            valid_indices.append(idx)
        else:
            rejected_indices.append(idx)
            rejection_reasons[idx] = reason

    # LOGIC — Build valid DataFrame (reset index for clean downstream use)
    valid_df = df.loc[valid_indices].copy().reset_index(drop=True)

    # LOGIC — Build rejected DataFrame with rejection_reason column appended
    if rejected_indices:
        rejected_df = df.loc[rejected_indices].copy()
        rejected_df["rejection_reason"] = rejected_df.index.map(rejection_reasons)
        rejected_df = rejected_df.reset_index(drop=True)
    else:
        # LOGIC — Return empty DataFrame with correct schema when no rejections
        rejected_df = df.iloc[0:0].copy()
        rejected_df["rejection_reason"] = pd.Series(dtype="str")

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df