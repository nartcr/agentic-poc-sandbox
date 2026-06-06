# BOILERPLATE
import logging
from datetime import datetime
from typing import Optional

import psycopg2
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def write_audit_record(
    db_credentials: dict,
    s3_key: str,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
    outcome: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped: int,
    error_message: Optional[str],
    report_s3_key: Optional[str],
    error_file_s3_key: Optional[str],
    service_identity: str,
) -> None:
    # LOGIC — ensure processing_timestamp is timezone-aware for TIMESTAMPTZ storage
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)

    # LOGIC — INSERT without ON CONFLICT: every processing attempt produces its own audit row
    insert_sql = """
        INSERT INTO demo_schema.pipeline_audit (
            s3_key,
            desk_code,
            trade_date,
            processing_timestamp,
            outcome,
            total_rows,
            rows_loaded,
            rows_rejected,
            rows_skipped_duplicate,
            error_message,
            report_s3_key,
            error_file_s3_key,
            service_identity
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    params = (
        s3_key,
        desk_code,
        trade_date,
        processing_timestamp,
        outcome,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped,
        error_message,
        report_s3_key,
        error_file_s3_key,
        service_identity,
    )

    # BOILERPLATE — open a dedicated connection for the audit write
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_credentials["host"],
            port=int(db_credentials["port"]),
            dbname=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, params)

        logger.info(
            "Audit record written: s3_key=%s desk_code=%s trade_date=%s outcome=%s "
            "total=%d loaded=%d rejected=%d skipped=%d service_identity=%s",
            s3_key,
            desk_code,
            trade_date,
            outcome,
            total_rows,
            rows_loaded,
            rows_rejected,
            rows_skipped,
            service_identity,
        )
    except Exception as exc:  # LOGIC — must not raise; log and continue per design
        logger.error(
            "Failed to write audit record for s3_key=%s desk_code=%s trade_date=%s: %s",
            s3_key,
            desk_code,
            trade_date,
            exc,
            exc_info=True,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass