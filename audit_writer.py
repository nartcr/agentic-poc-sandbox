# BOILERPLATE
import logging
import os
from datetime import date, datetime
from typing import Optional

import psycopg2

from db_connection import get_connection

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact SQL using column names from demo_schema.pipeline_audit
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows,
     rows_inserted, rows_rejected, error_message, processing_timestamp_et)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _parse_trade_date(trade_date: Optional[str]) -> Optional[date]:
    # LOGIC — convert trade_date string to datetime.date for DB insert;
    # returns None if input is None or unparseable (audit row still written)
    if trade_date is None:
        return None
    try:
        return datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(
            "audit_writer: could not parse trade_date '%s' as YYYY-MM-DD; "
            "storing NULL in audit row",
            trade_date,
        )
        return None


def write_audit_record(
    filename: str,
    desk_code: Optional[str],
    trade_date: Optional[str],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_timestamp_et: datetime,
) -> None:
    """
    Insert exactly one row into demo_schema.pipeline_audit.

    Parameters
    ----------
    filename               : source file name (e.g. DESK_2024-01-15_positions.csv)
    desk_code              : parsed desk code, or None if filename was unparseable
    trade_date             : YYYY-MM-DD string, or None if filename was unparseable
    status                 : one of "SUCCESS" | "PARTIAL" | "FAILURE"
    total_rows             : total rows in the source file (after header)
    rows_inserted          : rows actually inserted into trade_positions
    rows_rejected          : rows that failed row-level validation
    error_message          : pipeline error description if status == "FAILURE", else None
    processing_timestamp_et: ET-aware datetime captured at pipeline start
    """
    # LOGIC — convert string trade_date to date for TIMESTAMPTZ column
    trade_date_value = _parse_trade_date(trade_date)

    # LOGIC — validate status is one of the permitted values
    allowed_statuses = {"SUCCESS", "PARTIAL", "FAILURE"}
    if status not in allowed_statuses:
        raise ValueError(
            f"audit_writer: invalid status '{status}'; "
            f"must be one of {allowed_statuses}"
        )

    secret_id = os.environ["DB_SECRET_ID"]

    # BOILERPLATE — acquire connection via context manager; commits on clean exit,
    # rolls back on exception (handled inside get_connection)
    with get_connection(secret_id) as conn:
        with conn.cursor() as cursor:
            # LOGIC — insert audit row using exact column names from data contract
            cursor.execute(
                _INSERT_AUDIT_SQL,
                (
                    filename,
                    desk_code,
                    trade_date_value,
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    processing_timestamp_et,
                ),
            )
            logger.info(
                "Audit record written: filename=%s status=%s total_rows=%d "
                "rows_inserted=%d rows_rejected=%d",
                filename,
                status,
                total_rows,
                rows_inserted,
                rows_rejected,
            )