# BOILERPLATE
import logging
from datetime import datetime

import psycopg2

from secret_manager import get_db_credentials

logger = logging.getLogger(__name__)

# LOGIC — SQL uses exact column names from demo_schema.pipeline_audit data contract
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows, rows_inserted,
     rows_rejected, error_message, processing_timestamp_et)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(
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
    """
    Inserts one audit record into demo_schema.pipeline_audit.
    status must be one of: 'SUCCESS', 'PARTIAL', 'FAILED'.
    processing_timestamp_et must be a timezone-aware datetime in America/Toronto.
    """
    # LOGIC — validate status values as per data contract
    valid_statuses = {"SUCCESS", "PARTIAL", "FAILED"}
    if status not in valid_statuses:
        raise ValueError(
            f"Invalid status '{status}'. Must be one of: {sorted(valid_statuses)}"
        )

    # BOILERPLATE — retrieve credentials at runtime; never hardcoded
    creds = get_db_credentials()

    conn = None
    try:
        # BOILERPLATE — connect to Aurora PostgreSQL
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
            connect_timeout=10,
        )

        with conn.cursor() as cur:
            # LOGIC — execute INSERT with exact column order matching _INSERT_AUDIT_SQL
            cur.execute(
                _INSERT_AUDIT_SQL,
                (
                    filename,
                    desk_code,
                    trade_date,          # DATE column; None maps to NULL
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,       # TEXT column; None maps to NULL
                    processing_timestamp_et,  # TIMESTAMPTZ; tz-aware datetime
                ),
            )
            conn.commit()
            logger.info(
                "Audit record written: filename=%s status=%s total_rows=%d "
                "rows_inserted=%d rows_rejected=%d",
                filename,
                status,
                total_rows,
                rows_inserted,
                rows_rejected,
            )

    except psycopg2.Error as exc:
        logger.error(
            "Failed to write audit record for filename=%s: %s", filename, exc
        )
        if conn is not None:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise

    finally:
        # BOILERPLATE — always close the connection
        if conn is not None:
            try:
                conn.close()
            except psycopg2.Error:
                pass