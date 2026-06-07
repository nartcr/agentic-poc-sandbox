# BOILERPLATE
import logging
import pytz
from datetime import datetime

import psycopg2
import psycopg2.extensions
import psycopg2.extras

# BOILERPLATE
logger = logging.getLogger(__name__)
_ET = pytz.timezone("America/Toronto")


def _now_et() -> datetime:
    # LOGIC
    """Return current datetime in Eastern Time (America/Toronto)."""
    return datetime.now(_ET)


def start_audit_record(
    db_conn: psycopg2.extensions.connection,
    source_file_key: str,
    desk_code: str,
    trade_date: str,
    service_identity: str,
) -> int:
    # LOGIC
    """
    Insert a pipeline audit row with status='STARTED' and return the new audit_id.

    Columns written:
        source_file_key, desk_code, trade_date, status, service_identity, started_at

    Returns:
        audit_id (int) — the serial PK of the newly inserted row.

    Raises:
        psycopg2.DatabaseError — on any DB failure.
    """
    started_at = _now_et()

    # LOGIC — INSERT with RETURNING to capture the PK without a second round-trip
    sql = """
        INSERT INTO demo_schema.pipeline_audit
            (source_file_key, desk_code, trade_date, status, service_identity, started_at)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (
        source_file_key,
        desk_code,
        trade_date,        # PostgreSQL casts VARCHAR 'YYYY-MM-DD' → DATE automatically
        "STARTED",
        service_identity,
        started_at,
    )

    with db_conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            # Should never happen after a successful INSERT … RETURNING, but guard anyway
            raise psycopg2.DatabaseError(
                "INSERT INTO pipeline_audit returned no id — unexpected state."
            )
        audit_id: int = row[0]

    db_conn.commit()

    logger.info(
        "Audit record started: audit_id=%d source_file_key=%s desk_code=%s trade_date=%s",
        audit_id,
        source_file_key,
        desk_code,
        trade_date,
    )
    return audit_id


def complete_audit_record(
    db_conn: psycopg2.extensions.connection,
    audit_id: int,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message: str | None,
) -> None:
    # LOGIC
    """
    Update the pipeline audit row identified by audit_id with final processing results.

    Columns updated:
        status, total_rows, rows_loaded, rows_rejected, error_message, completed_at

    Args:
        db_conn        — open psycopg2 connection
        audit_id       — PK returned by start_audit_record()
        status         — "SUCCESS" or "FAILURE"
        total_rows     — total rows received from the source file
        rows_loaded    — rows actually inserted into trade_positions
        rows_rejected  — rows rejected by row_validator
        error_message  — None on success; human-readable error string on failure

    Raises:
        psycopg2.DatabaseError — on any DB failure.
        ValueError             — if status is not one of the accepted values.
    """
    # LOGIC — guard against unexpected status values
    if status not in ("SUCCESS", "FAILURE"):
        raise ValueError(
            f"Invalid status '{status}' passed to complete_audit_record. "
            "Expected 'SUCCESS' or 'FAILURE'."
        )

    completed_at = _now_et()

    # LOGIC — targeted UPDATE by PK; NULL-safe for optional fields
    sql = """
        UPDATE demo_schema.pipeline_audit
        SET
            status        = %s,
            total_rows    = %s,
            rows_loaded   = %s,
            rows_rejected = %s,
            error_message = %s,
            completed_at  = %s
        WHERE id = %s
    """
    params = (
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
        error_message,   # psycopg2 maps Python None → SQL NULL
        completed_at,
        audit_id,
    )

    with db_conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.rowcount != 1:
            # Warn if the expected row wasn't found — don't raise, to avoid masking
            # a more important upstream error already being handled by the caller.
            logger.warning(
                "complete_audit_record: expected to update 1 row for audit_id=%d "
                "but affected %d rows. The audit record may be missing.",
                audit_id,
                cur.rowcount,
            )

    db_conn.commit()

    logger.info(
        "Audit record completed: audit_id=%d status=%s total_rows=%s "
        "rows_loaded=%s rows_rejected=%s",
        audit_id,
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
    )