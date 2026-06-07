# BOILERPLATE
import json
import logging
from datetime import date, datetime
from typing import Optional

import psycopg2

import secret_client

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_MAX_ERROR_MESSAGE_LEN = 5000

# LOGIC — exact SQL from approved design
_INSERT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows, rows_inserted,
     rows_rejected, error_message, processing_timestamp_et, created_at)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
RETURNING audit_id
"""


def write_audit_record(
    filename: str,
    desk_code: Optional[str],
    trade_date: Optional[date],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_timestamp_et: datetime,
) -> int:
    # LOGIC — validate status is one of the allowed values
    allowed_statuses = {"SUCCESS", "FAILURE", "PARTIAL"}
    if status not in allowed_statuses:
        raise ValueError(
            f"Invalid status '{status}'. Must be one of {allowed_statuses}."
        )

    # LOGIC — TAC-7: enforce timezone-aware datetime before DB insert
    if processing_timestamp_et.tzinfo is None:
        raise ValueError(
            "processing_timestamp_et must be a timezone-aware datetime "
            "(tzinfo must not be None). Use pytz.timezone('America/Toronto')."
        )

    # LOGIC — truncate error_message to 5000 chars per design spec
    truncated_error: Optional[str] = None
    if error_message is not None:
        truncated_error = error_message[:_MAX_ERROR_MESSAGE_LEN]

    # BOILERPLATE — retrieve DB credentials from Secrets Manager at runtime
    creds = secret_client.get_db_credentials()

    conn = None
    cursor = None
    try:
        # BOILERPLATE — open psycopg2 connection using runtime credentials
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = False
        cursor = conn.cursor()

        # LOGIC — insert audit row; created_at handled server-side via now()
        cursor.execute(
            _INSERT_SQL,
            (
                filename,
                desk_code,
                trade_date,
                status,
                total_rows,
                rows_inserted,
                rows_rejected,
                truncated_error,
                processing_timestamp_et,
            ),
        )

        # LOGIC — capture auto-generated audit_id via RETURNING clause
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(
                "INSERT INTO demo_schema.pipeline_audit returned no audit_id."
            )
        audit_id: int = row[0]

        conn.commit()
        logger.info(
            "Audit record written: audit_id=%d filename=%s status=%s "
            "total_rows=%d rows_inserted=%d rows_rejected=%d",
            audit_id,
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
        return audit_id

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                logger.warning("Rollback failed after audit insert error.", exc_info=True)
        logger.error(
            "Failed to write audit record for filename=%s status=%s",
            filename,
            status,
            exc_info=True,
        )
        raise

    finally:
        # BOILERPLATE — always close cursor and connection
        if cursor is not None:
            try:
                cursor.close()
            except Exception:  # noqa: BLE001
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass