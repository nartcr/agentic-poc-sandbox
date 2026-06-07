# BOILERPLATE
import re
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# LOGIC — ordered validation rule codes
_REQUIRED_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")


def _validate_row(
    row: dict,
    row_number: int,
    filename_desk_code: str,
    filename_trade_date: str,
) -> tuple:
    # LOGIC — Rule 1: MISSING_FIELD
    for field in _REQUIRED_FIELDS:
        raw = row.get(field)
        if raw is None or str(raw).strip() == "":
            return (False, "MISSING_FIELD")

    # LOGIC — Rule 2: INVALID_TRADE_DATE
    trade_date_raw = row["trade_date"].strip()
    try:
        parsed_date = date.fromisoformat(trade_date_raw)
    except ValueError:
        return (False, "INVALID_TRADE_DATE")

    # LOGIC — Rule 3: INVALID_NOTIONAL
    notional_raw = row["notional_amount"].strip()
    try:
        notional_value = Decimal(notional_raw)
    except InvalidOperation:
        return (False, "INVALID_NOTIONAL")
    if not notional_value.is_finite() or notional_value <= Decimal("0"):
        return (False, "INVALID_NOTIONAL")

    # LOGIC — Rule 4: INVALID_CURRENCY
    currency_raw = row["currency"].strip()
    if not _CURRENCY_RE.match(currency_raw):
        return (False, "INVALID_CURRENCY")

    # LOGIC — Rule 5: DESK_CODE_MISMATCH
    row_desk_code = row["desk_code"].strip()
    if row_desk_code != filename_desk_code:
        return (False, "DESK_CODE_MISMATCH")

    return (True, "")


def validate_rows(
    rows: list,
    filename_desk_code: str,
    filename_trade_date: str,
) -> tuple:
    """
    Validate all raw CSV row dicts.

    Returns:
        (valid_rows, rejected_rows)
        valid_rows: list of dicts with typed fields ready for DB insert
        rejected_rows: list of input dicts augmented with row_number and rejection_reason
    """
    # BOILERPLATE
    valid_rows = []
    rejected_rows = []

    for row_number, row in enumerate(rows, start=1):
        # LOGIC — delegate per-row validation
        is_valid, rejection_reason = _validate_row(
            row, row_number, filename_desk_code, filename_trade_date
        )

        if is_valid:
            # LOGIC — build typed valid row dict
            trade_date_raw = row["trade_date"].strip()
            parsed_date = date.fromisoformat(trade_date_raw)
            notional_value = Decimal(row["notional_amount"].strip())

            valid_row = {
                "trade_id": row["trade_id"].strip(),
                "desk_code": row["desk_code"].strip(),
                "trade_date": parsed_date,
                "instrument_type": row["instrument_type"].strip(),
                "notional_amount": notional_value,
                "currency": row["currency"].strip().upper(),
                "counterparty_id": row["counterparty_id"].strip(),
            }
            valid_rows.append(valid_row)
            logger.debug(
                "Row %d passed validation: trade_id=%s",
                row_number,
                row.get("trade_id", ""),
            )
        else:
            # LOGIC — build rejected row dict with metadata
            rejected_row = dict(row)
            rejected_row["row_number"] = row_number
            rejected_row["rejection_reason"] = rejection_reason
            rejected_rows.append(rejected_row)
            logger.info(
                "Row %d rejected: reason=%s trade_id=%s",
                row_number,
                rejection_reason,
                row.get("trade_id", ""),
            )

    logger.info(
        "Validation complete: %d valid, %d rejected",
        len(valid_rows),
        len(rejected_rows),
    )
    return (valid_rows, rejected_rows)