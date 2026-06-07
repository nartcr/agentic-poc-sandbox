# BOILERPLATE
import logging
from datetime import datetime

import psycopg2

logger = logging.getLogger(__name__)

# LOGIC
def write_audit_record(
    conn: psycopg2.extensions.connection,
    source_file: str,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped: int,
    error_message,
    processed_at: datetime,
) -> None:
    """Write or update one audit row in demo_schema.pipeline_audit for the processed file.

    Uses INSERT ... ON CONFLICT (source_file) DO UPDATE so that re-runs overwrite
    the previous record rather than raising a duplicate-key error.
    """
    sql = """
        INSERT INTO demo_schema.pipeline_audit
            (source_file, status, total_rows, rows_loaded,
             rows_rejected, rows_skipped, error_message, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_file) DO UPDATE SET
            status        = EXCLUDED.status,
            total_rows    = EXCLUDED.total_rows,
            rows_loaded   = EXCLUDED.rows_loaded,
            rows_rejected = EXCLUDED.rows_rejected,
            rows_skipped  = EXCLUDED.rows_skipped,
            error_message = EXCLUDED.error_message,
            processed_at  = EXCLUDED.processed_at
    """

    params = (
        source_file,
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped,
        error_message,
        processed_at,
    )

    logger.info(
        "Writing audit record: source_file=%s status=%s total_rows=%d "
        "rows_loaded=%d rows_rejected=%d rows_skipped=%d",
        source_file,
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped,
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)

    # LOGIC: commit here so the audit record is persisted regardless of what
    # the caller does next with the connection
    conn.commit()

    logger.info(
        "Audit record committed for source_file=%s with status=%s",
        source_file,
        status,
    )