# BOILERPLATE
import json
import logging
from datetime import datetime
from typing import Optional

import pytz
import psycopg2

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE


# LOGIC
def write_audit_record(
    conn,
    source_key: str,
    desk_code: str,
    trade_date: str,
    status: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped: int,
    error_message: Optional[str],
    processing_start: datetime,
    processing_end: datetime,
    service_identity: str = "unknown",
) -> None:
    """
    Writes one audit row to demo_schema.pipeline_audit for each file processed.
    Does not raise on failure — logs the error instead.
    Commits immediately after insert.
    """
    # LOGIC: build the INSERT statement against the exact DDL column list
    sql = """
        INSERT INTO demo_schema.pipeline_audit (
            source_key,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_loaded,
            rows_rejected,
            rows_skipped,
            error_message,
            processing_start_et,
            processing_end_et,
            service_identity
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    # LOGIC: ensure timestamps are timezone-aware in America/Toronto
    # If a naive datetime is passed, localize it; if already aware, convert it.
    def _ensure_et(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return ET.localize(dt)
        return dt.astimezone(ET)

    start_et = _ensure_et(processing_start)
    end_et = _ensure_et(processing_end)

    # LOGIC: assemble the parameter tuple matching the column order above
    params = (
        source_key,
        desk_code,
        trade_date,
        status,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped,
        error_message,
        start_et,
        end_et,
        service_identity,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.info(
            "Audit record written: source_key=%s desk_code=%s trade_date=%s status=%s "
            "total_rows=%d rows_loaded=%d rows_rejected=%d rows_skipped=%d",
            source_key,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_loaded,
            rows_rejected,
            rows_skipped,
        )
    except Exception as exc:  # LOGIC: swallow — audit must never crash the pipeline
        logger.error(
            "Failed to write audit record for source_key=%s: %s",
            source_key,
            exc,
            exc_info=True,
        )