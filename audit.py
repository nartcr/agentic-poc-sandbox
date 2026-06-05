# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

import config

logger = logging.getLogger(__name__)

_AUDIT_SQL = """
INSERT INTO app.processing_audit
  (source_file, desk_code, trade_date, outcome, rows_received, rows_loaded, rows_rejected, error_detail, operator_identity, processed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(
    credentials: dict,
    source_file: str,
    desk_code: str,
    trade_date: str,
    outcome: str,
    rows_received: int,
    rows_loaded: int,
    rows_rejected: int,
    error_detail: str | None,
    operator_identity: str,
) -> None:
    """
    Write one audit record to app.processing_audit.

    Uses a separate DB connection to guarantee commit independent of loader transaction.
    outcome must be one of: 'SUCCESS', 'PARTIAL', 'FAILED'.
    """
    # LOGIC — processed_at in ET
    processed_at = datetime.now(pytz.timezone("America/Toronto"))

    conn = None
    try:
        # BOILERPLATE — separate connection for audit isolation
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            dbname=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
            options="-c search_path=app",
        )
        cursor = conn.cursor()

        # LOGIC — insert audit record
        cursor.execute(
            _AUDIT_SQL,
            (
                source_file,
                desk_code,
                trade_date,
                outcome,
                rows_received,
                rows_loaded,
                rows_rejected,
                error_detail,
                operator_identity,
                processed_at,
            ),
        )
        conn.commit()
        logger.info(
            "Audit record written: source_file=%s outcome=%s rows_received=%d rows_loaded=%d rows_rejected=%d",
            source_file,
            outcome,
            rows_received,
            rows_loaded,
            rows_rejected,
        )

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        # LOGIC — audit failures are logged but should not mask the original error
        logger.error(
            "Failed to write audit record for '%s': %s: %s",
            source_file,
            type(exc).__name__,
            exc,
        )
        raise

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass