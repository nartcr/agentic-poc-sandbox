# BOILERPLATE
import logging
import psycopg2

logger = logging.getLogger(__name__)

# LOGIC
def record_audit(conn: psycopg2.extensions.connection, audit_row: dict) -> None:
    """
    Writes one row to app.ingestion_audit per file processed.
    No conflict handling — each run appends a new record.
    Satisfies: BAC-7 (ET timestamp), BAC-8 (no credentials in module), NFR-3.3 (audit trail).
    """
    # LOGIC — exact column list from DATA CONTRACTS for app.ingestion_audit
    sql = """
        INSERT INTO app.ingestion_audit (
            source_file,
            desk_code,
            trade_date,
            status,
            rows_received,
            rows_loaded,
            rows_rejected,
            error_message,
            processed_at,
            report_s3_key,
            error_file_s3_key
        ) VALUES (
            %(source_file)s,
            %(desk_code)s,
            %(trade_date)s,
            %(status)s,
            %(rows_received)s,
            %(rows_loaded)s,
            %(rows_rejected)s,
            %(error_message)s,
            %(processed_at)s,
            %(report_s3_key)s,
            %(error_file_s3_key)s
        )
    """
    # LOGIC — use a dedicated cursor; caller owns commit/rollback
    with conn.cursor() as cursor:
        cursor.execute(sql, audit_row)

    logger.info(
        "Audit record written: source_file=%s status=%s rows_received=%d rows_loaded=%d rows_rejected=%d",
        audit_row.get("source_file"),
        audit_row.get("status"),
        audit_row.get("rows_received", 0),
        audit_row.get("rows_loaded", 0),
        audit_row.get("rows_rejected", 0),
    )