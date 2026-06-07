# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

import secret_manager

logger = logging.getLogger(__name__)

# BOILERPLATE — Eastern Time zone constant
_ET = pytz.timezone("America/Toronto")

# LOGIC — valid status values as defined in the design
_VALID_STATUSES = {"SUCCESS", "PARTIAL", "FAILED"}


# LOGIC
def write_audit_record(
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: str | None,
) -> None:
    """Insert a single audit record into demo_schema.pipeline_audit.

    Always commits — even if the main pipeline failed — so the audit trail
    is complete. Raises ValueError for an unrecognised status value so the
    caller knows the audit was not written.
    """
    # LOGIC — validate status before touching the DB
    if status not in _VALID_STATUSES:
        raise ValueError(
            f"Invalid audit status '{status}'. Must be one of: {_VALID_STATUSES}"
        )

    # LOGIC — ET-timezone-aware timestamp (TAC-7)
    processing_timestamp_et = datetime.now(_ET)

    # BOILERPLATE — retrieve credentials at runtime; no credentials in code
    credentials = secret_manager.get_db_credentials()

    conn = None
    try:
        # BOILERPLATE — open a dedicated connection for the audit write
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            dbname=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
        )
        with conn.cursor() as cur:
            # LOGIC — exact column list from demo_schema.pipeline_audit (YAML source of truth)
            cur.execute(
                """
                INSERT INTO demo_schema.pipeline_audit
                  (filename,
                   desk_code,
                   trade_date,
                   status,
                   total_rows,
                   rows_inserted,
                   rows_rejected,
                   error_message,
                   processing_timestamp_et)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    filename,
                    desk_code,
                    trade_date,  # psycopg2 coerces YYYY-MM-DD str to DATE
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    processing_timestamp_et,
                ),
            )
        # LOGIC — always commit the audit record regardless of pipeline outcome
        conn.commit()
        logger.info(
            "Audit record written: filename=%s desk_code=%s trade_date=%s "
            "status=%s total_rows=%d rows_inserted=%d rows_rejected=%d",
            filename,
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Rollback failed on audit connection", exc_info=True)
        logger.exception(
            "Failed to write audit record for filename=%s status=%s",
            filename,
            status,
        )
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Could not close audit DB connection", exc_info=True)