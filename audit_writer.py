# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import psycopg2

import secrets_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def write_audit_record(
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_timestamp_et: datetime,
) -> None:
    """Insert one audit record into demo_schema.pipeline_audit.

    Every invocation writes a new row (BIGSERIAL PK).  This is intentional —
    the table is an append-only audit log, not a state table.

    Args:
        filename: Original S3 object key of the processed file.
        desk_code: Parsed desk code, or None if filename parsing failed.
        trade_date: Parsed trade date as 'YYYY-MM-DD' string or None.
        status: 'SUCCESS' or 'FAILED'.
        total_rows: Total rows seen in the file (valid + rejected).
        rows_inserted: Rows actually committed to trade_positions.
        rows_rejected: Rows that failed validation.
        error_message: Exception message on failure; None on success.
        processing_timestamp_et: Timezone-aware datetime in America/Toronto.
    """
    # LOGIC — validate status value
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError(f"Invalid status value: {status!r}. Must be 'SUCCESS' or 'FAILED'.")

    # BOILERPLATE — retrieve credentials from Secrets Manager (never hardcoded)
    secret_id = os.environ["DB_SECRET_ID"]
    creds = secrets_client.get_secret(secret_id)

    host = creds["host"]
    port = int(creds["port"])
    username = creds["username"]
    password = creds["password"]
    dbname = creds["dbname"]

    # LOGIC — INSERT one audit row; column names must match demo_schema.pipeline_audit exactly
    sql = """
        INSERT INTO demo_schema.pipeline_audit
            (filename, desk_code, trade_date, status,
             total_rows, rows_inserted, rows_rejected,
             error_message, processing_timestamp_et)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = None
    try:
        # BOILERPLATE — open connection with timeout; do not leak connection on error
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            dbname=dbname,
            connect_timeout=10,
        )

        with conn.cursor() as cur:
            # LOGIC — bind parameters in column order; psycopg2 adapts Python types
            # to Postgres types (str → VARCHAR/DATE, None → NULL, datetime → TIMESTAMPTZ)
            cur.execute(
                sql,
                (
                    filename,
                    desk_code,
                    trade_date,
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    processing_timestamp_et,
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

    except Exception:
        # LOGIC — roll back on any DB error; re-raise so caller can handle
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Rollback failed after audit write error.", exc_info=True)
        logger.error(
            "Failed to write audit record for filename=%s", filename, exc_info=True
        )
        raise

    finally:
        # BOILERPLATE — always close the connection
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close DB connection after audit write.", exc_info=True)