# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/Toronto")


def write_audit_start(
    conn: psycopg2.extensions.connection,
    s3_key: str,
    desk_code: str,
    trade_date: str,
) -> int:
    # LOGIC — INSERT a new pipeline_audit row with status='STARTED'; return generated audit_id
    started_at = datetime.now(_ET)
    sql = """
        INSERT INTO demo_schema.pipeline_audit
            (s3_key, desk_code, trade_date, status, started_at)
        VALUES
            (%s, %s, %s, %s, %s)
        RETURNING audit_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, (s3_key, desk_code, trade_date, "STARTED", started_at))
        # LOGIC — retrieve the serial PK assigned by the database
        audit_id: int = cur.fetchone()[0]
    conn.commit()
    logger.info(
        "Audit start recorded: audit_id=%s desk_code=%s trade_date=%s s3_key=%s",
        audit_id,
        desk_code,
        trade_date,
        s3_key,
    )
    return audit_id


def write_audit_complete(
    conn: psycopg2.extensions.connection,
    audit_id: int,
    rows_received: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped: int,
    status: str,
    error_message: str | None,
) -> None:
    # LOGIC — UPDATE the existing audit row identified by audit_id with final state
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError(
            f"Invalid audit status '{status}': must be 'SUCCESS' or 'FAILED'"
        )

    completed_at = datetime.now(_ET)
    sql = """
        UPDATE demo_schema.pipeline_audit
        SET
            status        = %s,
            rows_received = %s,
            rows_loaded   = %s,
            rows_rejected = %s,
            rows_skipped  = %s,
            error_message = %s,
            completed_at  = %s
        WHERE
            audit_id = %s
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                status,
                rows_received,
                rows_loaded,
                rows_rejected,
                rows_skipped,
                error_message,  # None → NULL in postgres
                completed_at,
                audit_id,
            ),
        )
        # LOGIC — confirm exactly one row was updated; warn if the audit_id was not found
        if cur.rowcount != 1:
            logger.warning(
                "write_audit_complete updated %s rows for audit_id=%s (expected 1)",
                cur.rowcount,
                audit_id,
            )
    conn.commit()
    logger.info(
        "Audit complete recorded: audit_id=%s status=%s rows_received=%s "
        "rows_loaded=%s rows_rejected=%s rows_skipped=%s",
        audit_id,
        status,
        rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped,
    )