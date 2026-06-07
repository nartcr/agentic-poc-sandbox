# BOILERPLATE
import logging
from datetime import date, datetime

import psycopg2

logger = logging.getLogger(__name__)


# LOGIC
def write_audit_record(
    conn,
    s3_key: str,
    desk_code: str,
    trade_date: date,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped_dedup: int,
    processing_timestamp_et: datetime,
    service_name: str,
    error_message: str | None = None,
) -> None:
    """
    Inserts or updates one audit record in demo_schema.pipeline_audit.

    Uses INSERT ... ON CONFLICT (s3_key) DO UPDATE SET ... to support
    idempotent reprocessing (TAC-3, BAC-7, BAC-8).

    The s3_key unique constraint ensures exactly one audit row per file;
    a rerun updates the existing row with the latest outcome.
    """
    # LOGIC: parameterized upsert — ON CONFLICT (s3_key) DO UPDATE covers reprocessing
    upsert_sql = """
        INSERT INTO demo_schema.pipeline_audit (
            s3_key,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_loaded,
            rows_rejected,
            rows_skipped_dedup,
            processing_timestamp_et,
            service_name,
            error_message,
            updated_at
        )
        VALUES (
            %(s3_key)s,
            %(desk_code)s,
            %(trade_date)s,
            %(status)s,
            %(total_rows)s,
            %(rows_loaded)s,
            %(rows_rejected)s,
            %(rows_skipped_dedup)s,
            %(processing_timestamp_et)s,
            %(service_name)s,
            %(error_message)s,
            NOW()
        )
        ON CONFLICT (s3_key) DO UPDATE SET
            desk_code               = EXCLUDED.desk_code,
            trade_date              = EXCLUDED.trade_date,
            status                  = EXCLUDED.status,
            total_rows              = EXCLUDED.total_rows,
            rows_loaded             = EXCLUDED.rows_loaded,
            rows_rejected           = EXCLUDED.rows_rejected,
            rows_skipped_dedup      = EXCLUDED.rows_skipped_dedup,
            processing_timestamp_et = EXCLUDED.processing_timestamp_et,
            service_name            = EXCLUDED.service_name,
            error_message           = EXCLUDED.error_message,
            updated_at              = NOW()
    """

    # LOGIC: construct the parameter dict — all fields explicitly named
    params = {
        "s3_key": s3_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "status": status,
        "total_rows": total_rows,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_dedup": rows_skipped_dedup,
        "processing_timestamp_et": processing_timestamp_et,
        "service_name": service_name,
        "error_message": error_message,
    }

    logger.info(
        "Writing audit record: s3_key=%s status=%s total_rows=%d "
        "rows_loaded=%d rows_rejected=%d rows_skipped_dedup=%d",
        s3_key,
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped_dedup,
    )

    # BOILERPLATE: execute within a transaction; rollback on failure and re-raise
    try:
        with conn.cursor() as cursor:
            cursor.execute(upsert_sql, params)
        conn.commit()
        logger.info(
            "Audit record committed for s3_key=%s status=%s", s3_key, status
        )
    except psycopg2.Error as exc:
        logger.error(
            "Failed to write audit record for s3_key=%s: %s", s3_key, exc
        )
        conn.rollback()
        raise RuntimeError(
            f"Audit record write failed for s3_key={s3_key!r}: {exc}"
        ) from exc