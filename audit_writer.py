# BOILERPLATE
import logging
from datetime import datetime

import psycopg2

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC
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

_ERROR_MESSAGE_MAX_LEN = 2000


def write_audit_record(
    conn: psycopg2.extensions.connection,
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: str | None,
    processing_timestamp_et: datetime,
) -> None:
    """
    Write a single audit row to demo_schema.pipeline_audit.
    Called once per Lambda invocation regardless of success or failure.
    Commits the transaction for this write.

    :param conn: Open psycopg2 connection.
    :param filename: Original S3 object key / filename.
    :param desk_code: Parsed desk code, or None on early failure.
    :param trade_date: Parsed trade date string (YYYY-MM-DD), or None on early failure.
    :param status: One of 'SUCCESS', 'FAILURE', 'PARTIAL'.
    :param total_rows: Total rows received in the file.
    :param rows_inserted: Rows actually inserted into trade_positions.
    :param rows_rejected: Rows rejected by validation.
    :param error_message: Exception message (truncated to 2000 chars), or None.
    :param processing_timestamp_et: Timezone-aware datetime in America/Toronto.
    """
    # LOGIC — validate timezone awareness
    if processing_timestamp_et.tzinfo is None:
        logger.warning(
            "processing_timestamp_et has no tzinfo — audit record may store incorrect timezone. "
            "Caller must provide an America/Toronto-aware datetime."
        )

    # LOGIC — truncate error message to prevent oversized TEXT inserts
    truncated_error: str | None = None
    if error_message is not None:
        truncated_error = error_message[:_ERROR_MESSAGE_MAX_LEN]
        if len(error_message) > _ERROR_MESSAGE_MAX_LEN:
            logger.warning(
                "error_message truncated from %d to %d characters for audit record.",
                len(error_message),
                _ERROR_MESSAGE_MAX_LEN,
            )

    # LOGIC — build parameter dict using exact column names from schema
    params = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,        # PostgreSQL casts 'YYYY-MM-DD' str → DATE; None → NULL
        "status": status,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "error_message": truncated_error,
        "processing_timestamp_et": processing_timestamp_et,
    }

    # LOGIC — execute insert and commit
    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, params)
        conn.commit()
        logger.info(
            "Audit record written: filename=%s status=%s total_rows=%d "
            "rows_inserted=%d rows_rejected=%d",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except Exception as exc:
        # LOGIC — attempt rollback so the connection remains usable; log but re-raise
        # so pipeline_handler knows the audit write failed
        logger.error(
            "Failed to write audit record for filename=%s: %s",
            filename,
            exc,
            exc_info=True,
        )
        try:
            conn.rollback()
        except Exception as rollback_exc:
            logger.error(
                "Rollback after audit write failure also failed: %s",
                rollback_exc,
                exc_info=True,
            )
        raise