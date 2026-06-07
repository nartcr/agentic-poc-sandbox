# BOILERPLATE
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

# LOGIC — The seven mandatory columns every row must contain
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _check_mandatory_present(row: pd.Series, errors: list) -> None:
    # LOGIC — Rule 1: all seven mandatory fields must be present and non-null/non-empty
    for col in MANDATORY_COLUMNS:
        if col not in row.index:
            errors.append(f"{col}: missing")
            continue
        value = row[col]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            errors.append(f"{col}: missing")
        elif isinstance(value, str) and value.strip() == "":
            errors.append(f"{col}: missing")


def _check_trade_date_format(row: pd.Series, errors: list) -> None:
    # LOGIC — Rule 2: trade_date must parse as a valid YYYY-MM-DD date
    if "trade_date" not in row.index:
        return  # already caught by mandatory check
    value = row["trade_date"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if isinstance(value, str) and value.strip() == "":
        return  # already caught by mandatory check
    parsed = pd.to_datetime(str(value).strip(), format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed):
        errors.append("trade_date: invalid date format, expected YYYY-MM-DD")


def _check_notional_numeric(row: pd.Series, errors: list) -> None:
    # LOGIC — Rule 3: notional_amount must be coercible to float (not NaN after coercion)
    if "notional_amount" not in row.index:
        return  # already caught by mandatory check
    value = row["notional_amount"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if isinstance(value, str) and value.strip() == "":
        return  # already caught by mandatory check
    coerced = pd.to_numeric(value, errors="coerce")
    if pd.isna(coerced):
        errors.append("notional_amount: not numeric")


def _check_currency_length(row: pd.Series, errors: list) -> None:
    # LOGIC — Rule 4: currency must be exactly 3 characters
    if "currency" not in row.index:
        return  # already caught by mandatory check
    value = row["currency"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if isinstance(value, str) and value.strip() == "":
        return  # already caught by mandatory check
    if len(str(value).strip()) != 3:
        errors.append("currency: must be 3 characters")


def _check_trade_id_nonempty(row: pd.Series, errors: list) -> None:
    # LOGIC — Rule 5: trade_id must be a non-empty string
    # This is covered by mandatory check; here we explicitly confirm non-empty string type
    if "trade_id" not in row.index:
        return  # already caught by mandatory check
    value = row["trade_id"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if not isinstance(value, str):
        str_value = str(value).strip()
        if str_value == "" or str_value == "nan":
            errors.append("trade_id: must be non-empty string")
    else:
        if value.strip() == "":
            return  # already caught by mandatory check


def _check_desk_code_matches_filename(row: pd.Series, desk_code: str, errors: list) -> None:
    # LOGIC — Rule 6: row's desk_code must match the desk_code parsed from the filename
    if "desk_code" not in row.index:
        return  # already caught by mandatory check
    value = row["desk_code"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if isinstance(value, str) and value.strip() == "":
        return  # already caught by mandatory check
    if str(value).strip() != desk_code:
        errors.append(
            f"desk_code: value '{str(value).strip()}' does not match filename desk_code '{desk_code}'"
        )


def _check_trade_date_matches_filename(row: pd.Series, trade_date_str: str, errors: list) -> None:
    # LOGIC — Rule 7: row's trade_date value must match the trade_date parsed from the filename
    if "trade_date" not in row.index:
        return  # already caught by mandatory check
    value = row["trade_date"]
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return  # already caught by mandatory check
    if isinstance(value, str) and value.strip() == "":
        return  # already caught by mandatory check
    # Normalise: re-parse and reformat to YYYY-MM-DD for comparison
    parsed = pd.to_datetime(str(value).strip(), format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed):
        return  # invalid format already caught by _check_trade_date_format
    row_date_str = parsed.strftime("%Y-%m-%d")
    if row_date_str != trade_date_str:
        errors.append(
            f"trade_date: value '{row_date_str}' does not match filename trade_date '{trade_date_str}'"
        )


def _validate_single_row(row: pd.Series, desk_code: str, trade_date_str: str) -> list:
    # LOGIC — Runs all validation checks on a single row, returns list of error strings
    errors = []
    _check_mandatory_present(row, errors)
    # Only run format/value checks if the field was present and non-empty
    _check_trade_date_format(row, errors)
    _check_notional_numeric(row, errors)
    _check_currency_length(row, errors)
    _check_trade_id_nonempty(row, errors)
    _check_desk_code_matches_filename(row, desk_code, errors)
    _check_trade_date_matches_filename(row, trade_date_str, errors)
    return errors


def validate_rows(
    df: pd.DataFrame, desk_code: str, trade_date_str: str
) -> tuple:
    # LOGIC — Main entry point: validates all rows and splits into valid and rejected DataFrames
    logger.info(
        "Starting row validation: total_rows=%d desk_code=%s trade_date=%s",
        len(df),
        desk_code,
        trade_date_str,
    )

    if df.empty:
        logger.warning("Input DataFrame is empty — returning empty valid and rejected frames")
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        return df.copy(), rejected_df

    # LOGIC — Apply per-row validation and collect rejection reasons
    rejection_reasons = []
    for _, row in df.iterrows():
        errors = _validate_single_row(row, desk_code, trade_date_str)
        if errors:
            rejection_reasons.append("; ".join(errors))
        else:
            rejection_reasons.append(None)

    rejection_series = pd.Series(rejection_reasons, index=df.index)
    is_rejected = rejection_series.notna()

    # LOGIC — Split into valid and rejected DataFrames
    valid_df = df[~is_rejected].copy()
    rejected_df = df[is_rejected].copy()
    rejected_df["rejection_reason"] = rejection_series[is_rejected]

    # LOGIC — Coerce notional_amount to numeric in valid_df for safe downstream use
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        valid_df["notional_amount"] = pd.to_numeric(valid_df["notional_amount"], errors="coerce")

    logger.info(
        "Row validation complete: valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df