import json
import logging
import os
from datetime import datetime

import psycopg2

from exceptions import AuditWriteError
from secrets import DBCredentials

# BOILERPLATE
logger = logging.getLogger(__name__)


# LOGIC
def write_audit_record(
    credentials: DBCredentials,
    source_file: str,
    desk_code: str,
    trade_date: str,
    outcome: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_timestamp_et: datetime,
) -> None:
    """Write one audit record to demo_schema.pipeline_audit."""

    # LOGIC — read service identity from environment; never hardcoded
    service_identity = os.environ["SERVICE_IDENTITY"]

    # LOGIC — SQL insert for audit trail
    sql = """
        INSERT INTO demo_schema.pipeline_audit
          (source_file, desk_code, trade_date, outcome, total_rows, rows_inserted,
           rows_rejected, error_message, processing_timestamp_et, service_identity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    params = (
        source_file,
        desk_code,
        trade_date,
        outcome,
        total_rows,
        rows_inserted,
        rows_rejected,
        error_message,
        processing_timestamp_et,
        service_identity,
    )

    conn = None
    try:
        # BOILERPLATE — open psycopg2 connection from credentials
        conn = psycopg2.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.username,
            password=credentials.password,
            dbname=credentials.dbname,
        )
        with conn.cursor() as cur:
            # LOGIC — execute single audit row insert
            cur.execute(sql, params)
        conn.commit()
        logger.info(
            "Audit record written: source_file=%s outcome=%s",
            source_file,
            outcome,
        )
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error("Failed to write audit record: %s", exc, exc_info=True)
        raise AuditWriteError(f"Audit write failed: {exc}") from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass