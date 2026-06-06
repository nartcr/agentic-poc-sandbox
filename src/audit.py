import logging
import sqlalchemy

# BOILERPLATE
logger = logging.getLogger(__name__)


def write_audit_record(engine, audit_row: dict) -> None:
    # LOGIC — inserts one row into demo_schema.pipeline_audit per pipeline run.
    # No ON CONFLICT — every run produces exactly one audit row, even for reprocessed files.
    sql = sqlalchemy.text(
        """
        INSERT INTO demo_schema.pipeline_audit
            (run_id, s3_key, desk_code, trade_date, processing_timestamp,
             status, total_rows, rows_inserted, rows_rejected, rows_skipped_duplicate,
             report_s3_key, error_s3_key, service_identity)
        VALUES
            (:run_id, :s3_key, :desk_code, :trade_date, :processing_timestamp,
             :status, :total_rows, :rows_inserted, :rows_rejected, :rows_skipped_duplicate,
             :report_s3_key, :error_s3_key, :service_identity)
        """
    )

    # LOGIC — use a dedicated connection with autocommit-style commit so the audit
    # write is independent of the loader transaction.
    with engine.begin() as conn:
        conn.execute(sql, audit_row)

    logger.info(
        "Audit record written: run_id=%s status=%s",
        audit_row.get("run_id"),
        audit_row.get("status"),
    )