# BOILERPLATE
import json
import logging
import os

import psycopg2
import pytz
from datetime import datetime

logger = logging.getLogger(__name__)

# LOGIC
def write_audit_record(credentials: dict, audit_payload: dict) -> None:
    """
    Inserts one audit row into demo_schema.pipeline_audit for every pipeline
    invocation attempt.  Never upserts — each attempt gets its own row.

    Required audit_payload keys:
        file_name, desk_code, trade_date, total_rows, rows_loaded,
        rows_rejected, processing_status, error_file_s3_key (nullable),
        report_s3_key (nullable), processed_at (ET-aware datetime or ISO str),
        service_identity
    """
    # LOGIC — service_identity sourced from env per design spec
    service_identity = os.environ["SERVICE_IDENTITY"]

    # BOILERPLATE — build connection
    conn = None
    try:
        conn = psycopg2.connect(
            host=credentials["host"],
            port=int(credentials["port"]),
            dbname=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
        )
        cursor = conn.cursor()

        # LOGIC — plain INSERT; no ON CONFLICT; every attempt is recorded
        insert_sql = """
            INSERT INTO demo_schema.pipeline_audit (
                file_name,
                desk_code,
                trade_date,
                total_rows,
                rows_loaded,
                rows_rejected,
                processing_status,
                error_file_s3_key,
                report_s3_key,
                processed_at,
                service_identity
            ) VALUES (
                %(file_name)s,
                %(desk_code)s,
                %(trade_date)s,
                %(total_rows)s,
                %(rows_loaded)s,
                %(rows_rejected)s,
                %(processing_status)s,
                %(error_file_s3_key)s,
                %(report_s3_key)s,
                %(processed_at)s,
                %(service_identity)s
            )
        """

        # LOGIC — merge service_identity into payload dict for execution
        row = {
            "file_name": audit_payload["file_name"],
            "desk_code": audit_payload["desk_code"],
            "trade_date": audit_payload["trade_date"],
            "total_rows": int(audit_payload["total_rows"]),
            "rows_loaded": int(audit_payload["rows_loaded"]),
            "rows_rejected": int(audit_payload["rows_rejected"]),
            "processing_status": audit_payload["processing_status"],
            "error_file_s3_key": audit_payload.get("error_file_s3_key"),
            "report_s3_key": audit_payload.get("report_s3_key"),
            "processed_at": audit_payload["processed_at"],
            "service_identity": service_identity,
        }

        cursor.execute(insert_sql, row)
        conn.commit()

        logger.info(
            "Audit record written: file_name=%s processing_status=%s",
            audit_payload["file_name"],
            audit_payload["processing_status"],
        )

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # BOILERPLATE — swallow rollback errors on already-broken conn
                pass
        logger.exception(
            "Failed to write audit record for file_name=%s",
            audit_payload.get("file_name", "UNKNOWN"),
        )
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # BOILERPLATE — ensure connection is always closed
                pass