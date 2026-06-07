# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — SQL INSERT targeting demo_schema.pipeline_audit with all columns from the data contract.
# processing_timestamp_et is application-generated (ET) and passed as a bound parameter per TAC-7.
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit (
    filename,
    desk_code,
    trade_date,
    status,
    total_rows,
    rows_inserted,
    rows_rejected,
    error_message,
    processing_timestamp_et
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
"""


def write_audit_record(
    conn: psycopg2.extensions.connection,
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: str | None,
) -> None:
    # LOGIC — generate ET timestamp in application layer; never use DB-side NOW() for this column
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz)

    # BOILERPLATE
    cursor = conn.cursor()

    try:
        # LOGIC
        cursor.execute(
            _INSERT_AUDIT_SQL,
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
            "Audit record written. filename=%s status=%s total_rows=%d "
            "rows_inserted=%d rows_rejected=%d processing_timestamp_et=%s",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
            processing_timestamp_et.isoformat(),
        )
    except Exception:
        conn.rollback()
        logger.exception(
            "Failed to write audit record for filename=%s status=%s",
            filename,
            status,
        )
        raise
    finally:
        cursor.close()