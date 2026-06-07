# BOILERPLATE
import logging
from datetime import date, datetime

import psycopg2

logger = logging.getLogger(__name__)

# LOGIC
def write_audit_record(
    db_conn,
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_ts_et: datetime,
) -> None:
    """
    Inserts one audit row into demo_schema.pipeline_audit.

    Parameters
    ----------
    db_conn            : live psycopg2 connection (caller owns commit/close)
    filename           : original S3 object key (basename)
    desk_code          : parsed desk code, or None on early failure
    trade_date         : datetime.date, or None on early failure
    status             : "SUCCESS" | "FAILURE" | "PARTIAL"
    total_rows         : total rows parsed from the file (0 if not reached)
    rows_inserted      : net-new rows written to trade_positions (0 if not reached)
    rows_rejected      : rows that failed validation (0 if not reached)
    error_message      : exception/error text, or None
    processing_ts_et   : timezone-aware datetime in America/Toronto
    """
    # LOGIC — validate status is a known value
    valid_statuses = {"SUCCESS", "FAILURE", "PARTIAL"}
    if status not in valid_statuses:
        raise ValueError(f"Invalid audit status '{status}'. Must be one of {valid_statuses}.")

    # LOGIC — build and execute INSERT; audit_id and created_at are DB-generated
    sql = """
        INSERT INTO demo_schema.pipeline_audit
            (filename,
             desk_code,
             trade_date,
             status,
             total_rows,
             rows_inserted,
             rows_rejected,
             error_message,
             processing_timestamp_et)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    params = (
        filename,
        desk_code,          # VARCHAR(50) | NULL
        trade_date,         # DATE | NULL — psycopg2 handles datetime.date natively
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
        error_message,      # TEXT | NULL
        processing_ts_et,   # TIMESTAMPTZ — psycopg2 handles tz-aware datetime natively
    )

    try:
        with db_conn.cursor() as cursor:
            cursor.execute(sql, params)
        logger.info(
            "Audit record written: filename=%s status=%s total_rows=%d "
            "rows_inserted=%d rows_rejected=%d",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except psycopg2.Error as exc:
        # LOGIC — log and re-raise so the orchestrator can decide how to handle
        logger.error(
            "Failed to write audit record for filename=%s: %s",
            filename,
            exc,
            exc_info=True,
        )
        raise