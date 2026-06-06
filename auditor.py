# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

logger = logging.getLogger(__name__)


def write_audit_record(db_credentials: dict, audit_row: dict) -> None:
    # LOGIC — persist one audit record per file processed; no ON CONFLICT so every run appends
    processed_at = datetime.now(pytz.timezone("America/Toronto"))
    service_identity = os.environ["SERVICE_IDENTITY"]

    # LOGIC — extract audit table from audit_row metadata; table name is passed in by main.py
    audit_table = audit_row.get("_audit_table", "rfdh.pipeline_audit")

    sql = f"""
        INSERT INTO {audit_table} (
            file_key,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_loaded,
            rows_rejected,
            error_summary,
            processed_at,
            service_identity
        ) VALUES (
            %(file_key)s,
            %(desk_code)s,
            %(trade_date)s,
            %(status)s,
            %(total_rows)s,
            %(rows_loaded)s,
            %(rows_rejected)s,
            %(error_summary)s,
            %(processed_at)s,
            %(service_identity)s
        )
    """

    # LOGIC — build the parameter dict for the INSERT
    params = {
        "file_key": audit_row["file_key"],
        "desk_code": audit_row["desk_code"],
        "trade_date": audit_row["trade_date"],
        "status": audit_row["status"],
        "total_rows": audit_row["total_rows"],
        "rows_loaded": audit_row["rows_loaded"],
        "rows_rejected": audit_row["rows_rejected"],
        "error_summary": audit_row.get("error_summary"),
        "processed_at": processed_at,
        "service_identity": service_identity,
    }

    conn = None
    try:
        # BOILERPLATE — open psycopg2 connection from credentials dict
        conn = psycopg2.connect(
            host=db_credentials["host"],
            port=db_credentials["port"],
            dbname=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.info(
            "Audit record written for file_key=%s status=%s processed_at=%s",
            audit_row["file_key"],
            audit_row["status"],
            processed_at.strftime("%Y-%m-%dT%H:%M:%S%z"),
        )
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.exception(
            "Failed to write audit record for file_key=%s", audit_row.get("file_key")
        )
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass