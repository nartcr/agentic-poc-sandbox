import logging
from datetime import datetime

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — business columns that must all be present and non-empty
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _is_missing(value) -> bool:
    """
    # LOGIC
    A field is considered missing if it is NaN/None or an empty/whitespace-only string.
    """
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _validate_row(row: pd.Series) -> list:
    """
    # LOGIC
    Apply all four validation rules to a single row in order.
    Returns a list of rejection reason strings (empty list = row is valid).
    Multiple failures are all collected (no short-circuit).
    """
    reasons = []

    # RULE 1: Mandatory field presence
    for field in _MANDATORY_FIELDS:
        value = row.get(field, None)
        if _is_missing(value):
            reasons.append(f"MISSING_FIELD: {field}")

    # RULE 2: trade_date format must be YYYY-MM-DD
    # Only validate if not already flagged as missing
    trade_date_missing = any(
        r == "MISSING_FIELD: trade_date" for r in reasons
    )
    if not trade_date_missing:
        try:
            datetime.strptime(str(row["trade_date"]).strip(), "%Y-%m-%d")
        except ValueError:
            reasons.append("INVALID_DATE_FORMAT: trade_date")

    # RULE 3: notional_amount must be numeric and non-negative
    # Only validate if not already flagged as missing
    notional_missing = any(
        r == "MISSING_FIELD: notional_amount" for r in reasons
    )
    if not notional_missing:
        raw_notional = str(row["notional_amount"]).strip()
        try:
            notional_value = float(raw_notional)
            if notional_value < 0:
                reasons.append("INVALID_NOTIONAL: negative value")
        except ValueError:
            reasons.append("INVALID_NOTIONAL: not numeric")

    # RULE 4: currency must be exactly 3 characters (ISO 4217)
    # Only validate if not already flagged as missing
    currency_missing = any(
        r == "MISSING_FIELD: currency" for r in reasons
    )
    if not currency_missing:
        currency_value = str(row["currency"]).strip()
        if len(currency_value) != 3:
            reasons.append("INVALID_CURRENCY: must be 3 characters")

    return reasons


def validate_rows(df: pd.DataFrame) -> tuple:
    """
    # LOGIC
    Apply all data quality rules to the raw DataFrame and split into
    a validated set and a rejected set with rejection reasons.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from file_reader.read_position_file (all columns as str).

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (valid_df, rejected_df)

        valid_df  — rows passing all rules; notional_amount cast to float64,
                    trade_date cast to datetime.date.
        rejected_df — rows failing one or more rules; all original columns
                      preserved as strings plus a rejection_reason column
                      (reasons joined by "|").
    """
    if df.empty:
        # LOGIC — return typed empty DataFrames preserving column structure
        valid_df = df.copy()
        if "notional_amount" in valid_df.columns:
            valid_df["notional_amount"] = valid_df["notional_amount"].astype(
                "float64"
            )
        rejected_df = df.copy()
        rejected_df["rejection_reason"] = pd.Series(dtype=str)
        logger.info("validate_rows received an empty DataFrame; nothing to validate.")
        return valid_df, rejected_df

    # LOGIC — apply validation row-by-row, collecting all reasons
    rejection_reasons = []
    for _, row in df.iterrows():
        reasons = _validate_row(row)
        rejection_reasons.append("|".join(reasons) if reasons else "")

    reasons_series = pd.Series(rejection_reasons, index=df.index)

    # LOGIC — split into valid and rejected based on whether any reason was recorded
    valid_mask = reasons_series == ""
    rejected_mask = ~valid_mask

    valid_df = df[valid_mask].copy()
    rejected_df = df[rejected_mask].copy()
    rejected_df["rejection_reason"] = reasons_series[rejected_mask]

    valid_count = valid_mask.sum()
    rejected_count = rejected_mask.sum()
    logger.info(
        "Validation complete: %d valid rows, %d rejected rows (total input: %d)",
        valid_count,
        rejected_count,
        len(df),
    )

    # LOGIC — coerce types on valid DataFrame as specified in design
    if not valid_df.empty:
        valid_df["notional_amount"] = valid_df["notional_amount"].apply(
            lambda x: float(str(x).strip())
        ).astype("float64")

        valid_df["trade_date"] = pd.to_datetime(
            valid_df["trade_date"].str.strip(), format="%Y-%m-%d"
        ).dt.date

    return valid_df, rejected_df