# BOILERPLATE
import logging
from datetime import datetime, date

import psycopg2

logger = logging.getLogger(__name__)

# LOGIC — SQL for inserting a single audit record; plain INSERT with no conflict handling
# Every processing attempt creates a new row to preserve the full immutable audit trail
_INSERT_AUDIT_SQL = """
INSERT INTO rfdh.processing_audit (
    desk_code,
    trade_date,
    s3_key,
    processing_service_id,
    status,
    total_rows,
    rows_loaded,
    rows_rejected,
    error_message,
    processed_at
)
VALUES (
    %(desk_code)s,
    %(trade_date)s,
    %(s3_key)s,
    %(processing_service_id)s,
    %(status)s,
    %(total_rows)s,
    %(rows_loaded)s,
    %(rows_rejected)s,
    %(error_message)s,
    %(processed_at)s
)
"""


def write_audit_record(
    conn: psycopg2.extensions.connection,
    desk_code: str,
    trade_date: str,
    s3_key: str,
    processing_service_id: str,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: str | None,
    processed_at: datetime,
) -> None:
    # LOGIC — parse trade_date string (YYYYMMDD) to a date object for the DATE column
    trade_date_parsed: date = datetime.strptime(trade_date, "%Y%m%d").date()

    params = {
        "desk_code": desk_code,
        "trade_date": trade_date_parsed,
        "s3_key": s3_key,
        "processing_service_id": processing_service_id,
        "status": status,
        "total_rows": total_rows,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "processed_at": processed_at,
    }

    logger.info(
        "Writing audit record: desk_code=%s trade_date=%s status=%s s3_key=%s",
        desk_code,
        trade_date,
        status,
        s3_key,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_AUDIT_SQL, params)
        conn.commit()
        logger.info(
            "Audit record committed for desk_code=%s trade_date=%s status=%s",
            desk_code,
            trade_date,
            status,
        )
    except Exception:
        conn.rollback()
        logger.exception(
            "Failed to write audit record for desk_code=%s trade_date=%s; transaction rolled back",
            desk_code,
            trade_date,
        )
        raise