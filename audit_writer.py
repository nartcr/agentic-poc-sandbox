# BOILERPLATE
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_ET = pytz.timezone("America/Toronto")

# LOGIC — exact INSERT SQL against demo_schema.pipeline_audit using all columns per data contract
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows, rows_inserted,
     rows_rejected, error_message, processing_timestamp_et)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(
    conn,
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
    """Insert one audit row into demo_schema.pipeline_audit and commit immediately.

    Commits in isolation so the audit record persists even if the caller rolls
    back the main data-load transaction.  Never raises — errors are logged only.
    """  # LOGIC
    try:
        # LOGIC — ensure the timestamp is ET-aware; if caller passes naive datetime, localise it
        if processing_timestamp_et.tzinfo is None:
            processing_timestamp_et = _ET.localize(processing_timestamp_et)

        params = (
            filename,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
            error_message,
            processing_timestamp_et,
        )

        # LOGIC — use a cursor, execute the INSERT, then commit immediately
        with conn.cursor() as cursor:
            cursor.execute(_INSERT_AUDIT_SQL, params)
            # LOGIC — commit now so audit row is durable regardless of outer transaction state
            conn.commit()

        logger.info(
            "Audit record written. filename=%s status=%s total_rows=%d "
            "rows_inserted=%d rows_rejected=%d",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )

    except Exception as exc:  # LOGIC — do NOT re-raise; audit failure must not mask primary error
        logger.error(
            "Failed to write audit record for filename=%s status=%s error=%s",
            filename,
            status,
            str(exc),
            exc_info=True,
        )