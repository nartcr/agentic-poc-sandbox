# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

from src.ingestion.secrets import get_db_credentials

logger = logging.getLogger(__name__)

# BOILERPLATE — Eastern Time zone used for all timestamps
_ET = pytz.timezone("America/Toronto")

# LOGIC — maximum length for error_message column as per data contract
_ERROR_MESSAGE_MAX_LEN = 2000


def _get_connection():
    # BOILERPLATE — build a fresh psycopg2 connection from cached credentials
    creds = get_db_credentials()
    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )
    return conn


def start_audit_record(s3_key: str, desk_code: str, trade_date: str) -> int:
    # LOGIC — insert an IN_PROGRESS audit row and return the generated audit_id
    started_at = datetime.now(_ET)
    service_identity = os.environ["SERVICE_IDENTITY"]

    logger.info(
        "Starting audit record. s3_key=%s desk_code=%s trade_date=%s started_at=%s",
        s3_key,
        desk_code,
        trade_date,
        started_at.isoformat(),
    )

    # LOGIC
    insert_sql = """
        INSERT INTO rfdh.ingestion_audit
            (s3_key, desk_code, trade_date, outcome, started_at, service_identity)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        RETURNING audit_id
    """

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (
                        s3_key,
                        desk_code,
                        trade_date,
                        "IN_PROGRESS",
                        started_at,
                        service_identity,
                    ),
                )
                # LOGIC — retrieve the serial PK in the same round-trip
                audit_id = cur.fetchone()[0]
    finally:
        conn.close()

    logger.info("Audit record created. audit_id=%d", audit_id)
    return audit_id


def complete_audit_record(
    audit_id: int,
    rows_loaded: int,
    rows_rejected: int,
    outcome: str,
    error_message: str = None,
) -> None:
    # LOGIC — update the audit row with final outcome, counts, and completion timestamp
    completed_at = datetime.now(_ET)

    # LOGIC — truncate error_message to data contract maximum
    truncated_error_message = None
    if error_message is not None:
        truncated_error_message = str(error_message)[:_ERROR_MESSAGE_MAX_LEN]

    logger.info(
        "Completing audit record. audit_id=%d outcome=%s rows_loaded=%s rows_rejected=%s completed_at=%s",
        audit_id,
        outcome,
        rows_loaded,
        rows_rejected,
        completed_at.isoformat(),
    )

    # LOGIC
    update_sql = """
        UPDATE rfdh.ingestion_audit
        SET
            rows_loaded    = %s,
            rows_rejected  = %s,
            outcome        = %s,
            completed_at   = %s,
            error_message  = %s
        WHERE audit_id = %s
    """

    conn = _get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    update_sql,
                    (
                        rows_loaded,
                        rows_rejected,
                        outcome,
                        completed_at,
                        truncated_error_message,
                        audit_id,
                    ),
                )
                # LOGIC — warn if the audit_id was not found (no rows updated)
                if cur.rowcount == 0:
                    logger.warning(
                        "complete_audit_record updated 0 rows. audit_id=%d may not exist.",
                        audit_id,
                    )
    finally:
        conn.close()

    logger.info("Audit record completed. audit_id=%d outcome=%s", audit_id, outcome)