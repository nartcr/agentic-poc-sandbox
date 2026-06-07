# BOILERPLATE
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import pytz

# BOILERPLATE — import DBCredentials from secret_manager (defined in same package)
from secret_manager import DBCredentials

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")

# LOGIC — SQL for upsert into demo_schema.pipeline_audit
# ON CONFLICT (file_key) DO UPDATE satisfies TAC-3 idempotency:
# re-running the pipeline for the same file overwrites the previous audit row.
_UPSERT_SQL = """
INSERT INTO demo_schema.pipeline_audit (
    file_key,
    desk_code,
    trade_date,
    status,
    total_rows,
    rows_loaded,
    rows_rejected,
    error_message,
    processed_at_et,
    report_s3_key,
    error_s3_key,
    updated_at
)
VALUES (
    %(file_key)s,
    %(desk_code)s,
    %(trade_date)s,
    %(status)s,
    %(total_rows)s,
    %(rows_loaded)s,
    %(rows_rejected)s,
    %(error_message)s,
    %(processed_at_et)s,
    %(report_s3_key)s,
    %(error_s3_key)s,
    %(updated_at)s
)
ON CONFLICT (file_key)
DO UPDATE SET
    desk_code       = EXCLUDED.desk_code,
    trade_date      = EXCLUDED.trade_date,
    status          = EXCLUDED.status,
    total_rows      = EXCLUDED.total_rows,
    rows_loaded     = EXCLUDED.rows_loaded,
    rows_rejected   = EXCLUDED.rows_rejected,
    error_message   = EXCLUDED.error_message,
    processed_at_et = EXCLUDED.processed_at_et,
    report_s3_key   = EXCLUDED.report_s3_key,
    error_s3_key    = EXCLUDED.error_s3_key,
    updated_at      = EXCLUDED.updated_at
"""


def write_audit_record(
    credentials: DBCredentials,
    file_key: str,
    desk_code: str,
    trade_date: str,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: Optional[str],
    processed_at_et: datetime,
    report_s3_key: Optional[str],
    error_s3_key: Optional[str],
) -> None:
    # BOILERPLATE — ensure processed_at_et is ET-aware before storing
    if processed_at_et.tzinfo is None:
        processed_at_et = _ET.localize(processed_at_et)

    # LOGIC — updated_at is set to the same ET-aware timestamp as processed_at_et
    # so the upsert always records when the record was last written
    updated_at = processed_at_et

    params = {
        "file_key": file_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "status": status,
        "total_rows": total_rows,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "processed_at_et": processed_at_et,
        "report_s3_key": report_s3_key,
        "error_s3_key": error_s3_key,
        "updated_at": updated_at,
    }

    # BOILERPLATE — open a short-lived connection; Lambda does not use connection pooling
    conn = None
    try:
        conn = psycopg2.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.username,
            password=credentials.password,
            dbname=credentials.dbname,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            # LOGIC — execute the upsert; one row per file_key in pipeline_audit
            cur.execute(_UPSERT_SQL, params)
            row_count = cur.rowcount

        conn.commit()

        logger.info(
            "Audit record written — file_key=%s status=%s rows_affected=%d",
            file_key,
            status,
            row_count,
        )

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Rollback failed during audit write error handling", exc_info=True)
        logger.exception(
            "Failed to write audit record for file_key=%s status=%s", file_key, status
        )
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close DB connection after audit write", exc_info=True)