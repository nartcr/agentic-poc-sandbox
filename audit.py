# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

logger = logging.getLogger(__name__)

# LOGIC — required keys that must be present in the secrets dict
_REQUIRED_SECRET_KEYS = {"host", "port", "dbname", "username", "password"}

ET = pytz.timezone("America/Toronto")


def _build_connection(secrets: dict):
    # LOGIC — validate secrets dict before attempting connection
    missing = _REQUIRED_SECRET_KEYS - secrets.keys()
    if missing:
        raise RuntimeError(
            f"Secrets dict is missing required keys: {sorted(missing)}"
        )
    return psycopg2.connect(
        host=secrets["host"],
        port=int(secrets["port"]),
        dbname=secrets["dbname"],
        user=secrets["username"],
        password=secrets["password"],
    )


def start_audit(file_name: str, source_file_key: str, secrets: dict) -> int:
    """
    Insert an IN_PROGRESS row into rfdh.audit_log.
    Returns the generated audit_id (serial PK).
    Satisfies: BAC-7, BAC-8
    """
    # LOGIC — capture ET timestamp at the moment the pipeline starts
    started_at_et = datetime.now(ET).isoformat()
    service_identity = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")

    # LOGIC — INSERT with RETURNING to retrieve the generated audit_id
    sql = """
        INSERT INTO rfdh.audit_log (
            file_name,
            source_file_key,
            status,
            rows_received,
            rows_loaded,
            rows_rejected,
            error_message,
            started_at_et,
            completed_at_et,
            service_identity
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING audit_id
    """

    conn = None
    try:
        conn = _build_connection(secrets)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        file_name,          # file_name
                        source_file_key,    # source_file_key
                        "IN_PROGRESS",      # status
                        None,               # rows_received — unknown at start
                        None,               # rows_loaded   — unknown at start
                        None,               # rows_rejected — unknown at start
                        None,               # error_message — none at start
                        started_at_et,      # started_at_et (ET ISO 8601)
                        None,               # completed_at_et — not yet complete
                        service_identity,   # service_identity (Lambda fn name)
                    ),
                )
                row = cur.fetchone()
                audit_id = row[0]

        logger.info(
            "Audit started: audit_id=%s file_name=%s started_at_et=%s",
            audit_id,
            file_name,
            started_at_et,
        )
        return audit_id

    except Exception:
        logger.exception(
            "Failed to insert audit row for file_name=%s", file_name
        )
        raise
    finally:
        if conn is not None:
            conn.close()


def complete_audit(
    audit_id: int,
    status: str,
    rows_received: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: str | None,
    secrets: dict,
) -> None:
    """
    Update the rfdh.audit_log row identified by audit_id with final
    status, row counts, completion timestamp, and optional error message.
    Satisfies: BAC-7, BAC-8
    """
    # LOGIC — capture ET timestamp at the moment the pipeline completes
    completed_at_et = datetime.now(ET).isoformat()

    # LOGIC — UPDATE the existing audit row; all nullable columns are now known
    sql = """
        UPDATE rfdh.audit_log
        SET
            status           = %s,
            rows_received    = %s,
            rows_loaded      = %s,
            rows_rejected    = %s,
            error_message    = %s,
            completed_at_et  = %s
        WHERE audit_id = %s
    """

    conn = None
    try:
        conn = _build_connection(secrets)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        status,           # 'SUCCESS' or 'FAILURE'
                        rows_received,
                        rows_loaded,
                        rows_rejected,
                        error_message,    # None if successful
                        completed_at_et,  # ET ISO 8601
                        audit_id,
                    ),
                )
                if cur.rowcount == 0:
                    logger.warning(
                        "complete_audit: no row updated for audit_id=%s "
                        "(row may not exist)",
                        audit_id,
                    )

        logger.info(
            "Audit completed: audit_id=%s status=%s rows_received=%s "
            "rows_loaded=%s rows_rejected=%s completed_at_et=%s",
            audit_id,
            status,
            rows_received,
            rows_loaded,
            rows_rejected,
            completed_at_et,
        )

    except Exception:
        logger.exception(
            "Failed to update audit row for audit_id=%s", audit_id
        )
        raise
    finally:
        if conn is not None:
            conn.close()