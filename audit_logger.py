# BOILERPLATE
import logging
from datetime import datetime
from typing import Optional

import psycopg2

from time_utils import to_et_isoformat

logger = logging.getLogger(__name__)

# LOGIC — SQL for the audit INSERT.
# All columns are explicitly named to match demo_schema.pipeline_audit exactly.
# audit_id is BIGSERIAL (auto-generated); created_at has DEFAULT now() — both omitted.
_INSERT_AUDIT_SQL = """
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
)
VALUES (
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


def write_audit_record(
    conn,
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
    # LOGIC — writes exactly one row to demo_schema.pipeline_audit.
    # Called inside a finally block in pipeline_handler.py so it always executes.
    # The connection and its transaction lifecycle are owned by the caller.
    # This function executes and commits the audit row independently so that
    # an upstream rollback (e.g. db_loader failure) does not suppress the audit record.

    if status not in ("SUCCESS", "FAILURE"):
        raise ValueError(f"status must be 'SUCCESS' or 'FAILURE', got: {status!r}")

    # LOGIC — trade_date may be a string "YYYY-MM-DD" or None.
    # psycopg2 accepts ISO date strings and None (→ SQL NULL) natively.
    et_iso = to_et_isoformat(processing_timestamp_et)

    params = {
        "filename": filename,
        "desk_code": desk_code,          # NULL when filename parse failed
        "trade_date": trade_date,        # NULL when filename parse failed
        "status": status,
        "total_rows": total_rows if total_rows is not None else 0,
        "rows_inserted": rows_inserted if rows_inserted is not None else 0,
        "rows_rejected": rows_rejected if rows_rejected is not None else 0,
        "error_message": error_message,
        "processing_timestamp_et": et_iso,
    }

    logger.info(
        "Writing audit record: filename=%s status=%s total_rows=%d "
        "rows_inserted=%d rows_rejected=%d",
        filename,
        status,
        params["total_rows"],
        params["rows_inserted"],
        params["rows_rejected"],
    )

    try:
        # LOGIC — use a dedicated cursor; commit separately from the main data
        # transaction so audit always persists even if the caller rolled back.
        with conn.cursor() as cursor:
            cursor.execute(_INSERT_AUDIT_SQL, params)
        conn.commit()
        logger.info("Audit record committed for filename=%s", filename)
    except psycopg2.Error as exc:
        logger.error(
            "Failed to write audit record for filename=%s: %s",
            filename,
            exc,
            exc_info=True,
        )
        # LOGIC — attempt rollback of the failed audit write to leave the
        # connection in a clean state; then re-raise so the caller is aware.
        try:
            conn.rollback()
        except psycopg2.Error as rb_exc:
            logger.error("Rollback after audit failure also failed: %s", rb_exc)
        raise