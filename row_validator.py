# BOILERPLATE
import logging
import re
from decimal import Decimal, InvalidOperation

import pandas as pd

from ingestion_exceptions import ValidationError
import file_writer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — all seven mandatory columns per data contract
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — currency must be exactly 3 uppercase alphabetic characters
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")

# LOGIC — trade_date must be YYYY-MM-DD
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_rows(
    df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    bucket: str,
) -> tuple:
    # LOGIC — check that all mandatory columns are present before row-level validation
    for col in MANDATORY_COLUMNS:
        if col not in df.columns:
            logger.error(
                "Mandatory column '%s' missing from file; rejecting entire file", col
            )
            raise ValidationError(f"MISSING_COLUMN:{col}")

    logger.info(
        "Validating %d rows for desk_code='%s', trade_date='%s'",
        len(df),
        desk_code,
        trade_date,
    )

    # LOGIC — apply row-level validation; collect rejection reasons
    rejection_reasons = []
    for _, row in df.iterrows():
        reason = _validate_single_row(row, desk_code, trade_date)
        rejection_reasons.append(reason)

    # LOGIC — split DataFrame into valid and rejected sets
    rejection_mask = pd.Series(
        [reason is not None for reason in rejection_reasons], index=df.index
    )

    valid_df = df[~rejection_mask].copy()
    rejected_df = df[rejection_mask].copy()

    # LOGIC — append rejection_reason column to rejected set
    rejected_df = rejected_df.copy()
    rejected_df["rejection_reason"] = [
        reason for reason in rejection_reasons if reason is not None
    ]

    valid_count = len(valid_df)
    rejected_count = len(rejected_df)

    logger.info(
        "Validation complete: %d valid rows, %d rejected rows",
        valid_count,
        rejected_count,
    )

    # LOGIC — write rejected rows to S3 even if empty (idempotent overwrite)
    if rejected_count > 0:
        error_key = file_writer.write_rejected_rows(
            bucket, desk_code, trade_date, rejected_df
        )
        logger.info("Rejected rows written to s3://%s/%s", bucket, error_key)
    else:
        logger.info("No rejected rows; skipping error file write")

    return valid_df, rejected_df


def _validate_single_row(
    row: pd.Series, desk_code: str, trade_date: str
) -> "str | None":
    # LOGIC — validate trade_id: non-null, non-empty string
    trade_id_val = row.get("trade_id")
    if trade_id_val is None or (isinstance(trade_id_val, float)) or str(trade_id_val).strip() == "" or trade_id_val != trade_id_val:
        return "INVALID_FIELD:trade_id:must_be_non_empty_string"

    # LOGIC — validate desk_code: non-null, non-empty, must match filename desk_code
    row_desk_code = row.get("desk_code")
    if row_desk_code is None or str(row_desk_code).strip() == "" or row_desk_code != row_desk_code:
        return "INVALID_FIELD:desk_code:must_be_non_empty_string"
    if str(row_desk_code).strip() != desk_code:
        return "INVALID_FIELD:desk_code:must_match_filename"

    # LOGIC — validate trade_date: non-null, valid YYYY-MM-DD, must match filename trade_date
    row_trade_date = row.get("trade_date")
    if row_trade_date is None or str(row_trade_date).strip() == "" or row_trade_date != row_trade_date:
        return "INVALID_FIELD:trade_date:must_be_non_null"
    trade_date_str = str(row_trade_date).strip()
    if not _DATE_PATTERN.match(trade_date_str):
        return "INVALID_FIELD:trade_date:must_be_YYYY-MM-DD"
    # LOGIC — validate month and day ranges to catch structurally matching but invalid dates
    try:
        from datetime import date as _date
        parts = trade_date_str.split("-")
        _date(int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        return "INVALID_FIELD:trade_date:invalid_calendar_date"
    if trade_date_str != trade_date:
        return "INVALID_FIELD:trade_date:must_match_filename"

    # LOGIC — validate instrument_type: non-null, non-empty string
    instrument_type_val = row.get("instrument_type")
    if (
        instrument_type_val is None
        or instrument_type_val != instrument_type_val
        or str(instrument_type_val).strip() == ""
    ):
        return "INVALID_FIELD:instrument_type:must_be_non_empty_string"

    # LOGIC — validate notional_amount: non-null, castable to Decimal, must be > 0
    notional_val = row.get("notional_amount")
    if notional_val is None or notional_val != notional_val or str(notional_val).strip() == "":
        return "INVALID_FIELD:notional_amount:must_be_non_null"
    try:
        notional_decimal = Decimal(str(notional_val).strip())
    except InvalidOperation:
        return "INVALID_FIELD:notional_amount:must_be_numeric"
    if notional_decimal <= Decimal("0"):
        return "INVALID_FIELD:notional_amount:must_be_greater_than_zero"

    # LOGIC — validate currency: non-null, exactly 3 uppercase alphabetic characters
    currency_val = row.get("currency")
    if currency_val is None or currency_val != currency_val or str(currency_val).strip() == "":
        return "INVALID_FIELD:currency:must_be_non_null"
    if not _CURRENCY_PATTERN.match(str(currency_val).strip()):
        return "INVALID_FIELD:currency:must_be_3_char_alpha"

    # LOGIC — validate counterparty_id: non-null, non-empty string
    counterparty_val = row.get("counterparty_id")
    if (
        counterparty_val is None
        or counterparty_val != counterparty_val
        or str(counterparty_val).strip() == ""
    ):
        return "INVALID_FIELD:counterparty_id:must_be_non_empty_string"

    # LOGIC — all checks passed; row is valid
    return None