# BOILERPLATE
import io
import logging
import re
from datetime import datetime

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — mandatory columns as specified in the data contract
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — ISO 4217 currency format: exactly 3 uppercase alphabetic characters
_CURRENCY_PATTERN = re.compile(r'^[A-Z]{3}$')

# LOGIC — trade_date must be YYYY-MM-DD
_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _is_valid_date(value: str) -> bool:
    """
    Returns True if value is a string parseable as a valid YYYY-MM-DD date.
    """
    # LOGIC
    if not isinstance(value, str) or not _DATE_PATTERN.match(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _check_row(row: pd.Series, desk_code: str, trade_date: str) -> str:
    """
    Applies all field-level validation rules to a single row.
    Returns the rejection reason string for the first failing rule,
    or empty string if the row is valid.
    """
    # LOGIC — trade_id: non-null, non-empty string
    trade_id_val = row.get("trade_id")
    if pd.isnull(trade_id_val) or str(trade_id_val).strip() == "":
        return "trade_id: must be a non-empty string"

    # LOGIC — desk_code: non-null, non-empty, must match filename-derived desk_code
    row_desk_code = row.get("desk_code")
    if pd.isnull(row_desk_code) or str(row_desk_code).strip() == "":
        return "desk_code: must be a non-empty string"
    if str(row_desk_code).strip() != desk_code:
        return (
            f"desk_code: value '{row_desk_code}' does not match "
            f"filename-derived desk_code '{desk_code}'"
        )

    # LOGIC — trade_date: non-null, must parse as YYYY-MM-DD, must match filename-derived trade_date
    row_trade_date = row.get("trade_date")
    if pd.isnull(row_trade_date) or str(row_trade_date).strip() == "":
        return "trade_date: must be a non-null YYYY-MM-DD date string"
    row_trade_date_str = str(row_trade_date).strip()
    if not _is_valid_date(row_trade_date_str):
        return "trade_date: must parse as a valid YYYY-MM-DD date"
    if row_trade_date_str != trade_date:
        return (
            f"trade_date: value '{row_trade_date_str}' does not match "
            f"filename-derived trade_date '{trade_date}'"
        )

    # LOGIC — instrument_type: non-null, non-empty string
    instrument_type_val = row.get("instrument_type")
    if pd.isnull(instrument_type_val) or str(instrument_type_val).strip() == "":
        return "instrument_type: must be a non-empty string"

    # LOGIC — notional_amount: must be parseable as float and must be > 0
    notional_val = row.get("notional_amount")
    if pd.isnull(notional_val):
        return "notional_amount: not a positive number"
    try:
        notional_float = float(notional_val)
    except (ValueError, TypeError):
        return "notional_amount: not a positive number"
    if notional_float <= 0:
        return "notional_amount: not a positive number"

    # LOGIC — currency: exactly 3 uppercase alphabetic characters
    currency_val = row.get("currency")
    if pd.isnull(currency_val) or str(currency_val).strip() == "":
        return "currency: must be 3 uppercase alpha characters"
    if not _CURRENCY_PATTERN.match(str(currency_val).strip()):
        return "currency: must be 3 uppercase alpha characters"

    # LOGIC — counterparty_id: non-null, non-empty string
    counterparty_val = row.get("counterparty_id")
    if pd.isnull(counterparty_val) or str(counterparty_val).strip() == "":
        return "counterparty_id: must be a non-empty string"

    # LOGIC — all checks passed
    return ""


def validate_rows(
    raw_bytes: bytes,
    desk_code: str,
    trade_date: str,
) -> tuple:
    """
    Parses raw_bytes as UTF-8 CSV.
    Applies field-level validation to each row.
    Returns (valid_df, rejected_df).
    valid_df columns: trade_id, desk_code, trade_date, instrument_type,
                      notional_amount (float), currency, counterparty_id
    rejected_df columns: all input columns + rejection_reason (str)
    """
    # LOGIC — parse CSV from raw bytes using UTF-8 encoding
    try:
        raw_df = pd.read_csv(
            io.BytesIO(raw_bytes),
            encoding="utf-8",
            dtype=str,          # read all columns as strings initially; coerce types after validation
            keep_default_na=False,
            na_values=["", "NA", "N/A", "null", "NULL", "None", "NaN"],
        )
    except Exception as exc:
        logger.error("Failed to parse CSV bytes: %s", exc)
        raise ValueError(f"CSV parse error: {exc}") from exc

    logger.info(
        "Parsed CSV with %d rows and columns: %s",
        len(raw_df), list(raw_df.columns)
    )

    # LOGIC — check that all mandatory columns are present
    missing_cols = [col for col in _MANDATORY_COLUMNS if col not in raw_df.columns]
    if missing_cols:
        raise ValueError(
            f"CSV is missing mandatory columns: {missing_cols}"
        )

    if raw_df.empty:
        logger.warning("CSV file contains no data rows.")
        valid_df = pd.DataFrame(columns=_MANDATORY_COLUMNS)
        rejected_df = pd.DataFrame(columns=list(raw_df.columns) + ["rejection_reason"])
        return valid_df, rejected_df

    # LOGIC — apply row-level validation; collect rejection reasons
    rejection_reasons = []
    for _, row in raw_df.iterrows():
        reason = _check_row(row, desk_code, trade_date)
        rejection_reasons.append(reason)

    raw_df = raw_df.copy()
    raw_df["_rejection_reason"] = rejection_reasons

    # LOGIC — split into valid and rejected sets
    valid_mask = raw_df["_rejection_reason"] == ""
    rejected_mask = ~valid_mask

    valid_rows = raw_df[valid_mask].copy()
    rejected_rows = raw_df[rejected_mask].copy()

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        len(valid_rows), len(rejected_rows)
    )

    # LOGIC — build valid_df with exactly the 7 mandatory columns, types coerced
    valid_df = valid_rows[_MANDATORY_COLUMNS].copy()
    # LOGIC — coerce notional_amount to float for downstream use
    valid_df["notional_amount"] = pd.to_numeric(
        valid_df["notional_amount"], errors="coerce"
    )
    # LOGIC — strip whitespace from string columns
    for col in ["trade_id", "desk_code", "trade_date", "instrument_type", "currency", "counterparty_id"]:
        valid_df[col] = valid_df[col].str.strip()

    valid_df = valid_df.reset_index(drop=True)

    # LOGIC — build rejected_df with all original columns plus rejection_reason
    # Drop the internal _rejection_reason column and rename to rejection_reason
    rejected_df = rejected_rows.drop(columns=["_rejection_reason"]).copy()
    rejected_df["rejection_reason"] = rejected_rows["_rejection_reason"].values
    rejected_df = rejected_df.reset_index(drop=True)

    return valid_df, rejected_df