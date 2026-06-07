# BOILERPLATE
import logging
import os
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extensions
import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET_TZ = pytz.timezone("America/Toronto")

_INSERT_SQL = """
    INSERT INTO demo_schema.pipeline_audit (
        filename,
        desk_code,
        trade_date,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
        error_message,
        processing_timestamp_et
    ) VALUES (
        %(filename)s,
        %(desk_code)s,
        %(trade_date)s,
        %(status)s,
        %(total_rows)s,
        %(rows_inserted)s,
        %(rows_rejected)s,
        %(error_message)s,
        %(processing_timestamp_et)s
    )
"""


def _current_et_timestamp() -> datetime:
    # LOGIC — returns a timezone-aware datetime in America/Toronto
    return datetime.now(_ET_TZ)


def _coerce_trade_date(trade_date_str: Optional[str]):
    # LOGIC — converts YYYY-MM-DD string to date object; returns None if input is None or unparseable
    if trade_date_str is None:
        return None
    try:
        return datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("audit_writer: could not parse trade_date_str=%r — storing NULL", trade_date_str)
        return None


def write_audit_record(
    conn: psycopg2.extensions.connection,
    filename: str,
    desk_code: Optional[str],
    trade_date_str: Optional[str],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
) -> None:
    # LOGIC — validates status value before writing
    valid_statuses = {"SUCCESS", "PARTIAL", "FAILURE"}
    if status not in valid_statuses:
        raise ValueError(
            f"audit_writer: invalid status {status!r}; must be one of {valid_statuses}"
        )

    processing_ts_et = _current_et_timestamp()
    trade_date_value = _coerce_trade_date(trade_date_str)

    params = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_value,
        "status": status,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "processing_timestamp_et": processing_ts_et,
    }

    logger.info(
        "audit_writer: inserting audit record filename=%r status=%r total_rows=%d "
        "rows_inserted=%d rows_rejected=%d processing_timestamp_et=%s",
        filename,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
        processing_ts_et.isoformat(),
    )

    # LOGIC — use a dedicated cursor; commit independently so the audit row
    # persists even if the caller subsequently rolls back the main transaction
    with conn.cursor() as cur:
        cur.execute(_INSERT_SQL, params)

    # LOGIC — independent commit: audit record must survive outer rollback
    conn.commit()

    logger.info(
        "audit_writer: audit record committed for filename=%r status=%r",
        filename,
        status,
    )