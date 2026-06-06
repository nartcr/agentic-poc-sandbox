# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

import secrets

logger = logging.getLogger(__name__)

# LOGIC — SQL upsert for idempotent audit record per (desk_code, trade_date)
_UPSERT_SQL = """
INSERT INTO rfdh.pipeline_audit (
    desk_code,
    trade_date,
    status,
    total_rows_received,
    rows_inserted,
    rows_rejected,
    rows_skipped_duplicate,
    processing_timestamp_et,
    s3_input_key,
    error_message,
    service_identity
)
VALUES (
    %(desk_code)s,
    %(trade_date)s,
    %(status)s,
    %(total_rows_received)s,
    %(rows_inserted)s,
    %(rows_rejected)s,
    %(rows_skipped_duplicate)s,
    %(processing_timestamp_et)s,
    %(s3_input_key)s,
    %(error_message)s,
    %(service_identity)s
)
ON CONFLICT (desk_code, trade_date) DO UPDATE SET
    status                 = EXCLUDED.status,
    total_rows_received    = EXCLUDED.total_rows_received,
    rows_inserted          = EXCLUDED.rows_inserted,
    rows_rejected          = EXCLUDED.rows_rejected,
    rows_skipped_duplicate = EXCLUDED.rows_skipped_duplicate,
    processing_timestamp_et = EXCLUDED.processing_timestamp_et,
    error_message          = EXCLUDED.error_message
"""


def record(
    desk_code: str,
    trade_date: str,
    summary: dict,
    status: str,
    error_message: str = None,
) -> None:
    # LOGIC — generate ET timestamp at the moment record() is called
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz)

    # BOILERPLATE — fetch credentials at call time; no caching
    creds = secrets.get_db_credentials()

    # LOGIC — extract audit fields from summary dict; default to None if absent
    total_rows_received = summary.get("total_rows_received")
    rows_inserted = summary.get("rows_inserted")
    rows_rejected = summary.get("rows_rejected")
    rows_skipped_duplicate = summary.get("rows_skipped_duplicate")
    s3_input_key = summary.get("s3_input_key")

    # BOILERPLATE — read service identity from environment
    service_identity = os.environ["SERVICE_IDENTITY"]

    # LOGIC — build parameter dict for named-parameter SQL substitution
    params = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "status": status,
        "total_rows_received": total_rows_received,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "processing_timestamp_et": processing_timestamp_et,
        "s3_input_key": s3_input_key,
        "error_message": error_message,
        "service_identity": service_identity,
    }

    logger.info(
        "Recording audit entry for desk_code=%s trade_date=%s status=%s",
        desk_code,
        trade_date,
        status,
    )

    # BOILERPLATE — open connection, execute upsert, commit, close
    conn = None
    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        with conn:
            with conn.cursor() as cur:
                # LOGIC — upsert audit row; ON CONFLICT updates all mutable fields
                cur.execute(_UPSERT_SQL, params)
                logger.info(
                    "Audit record upserted for desk_code=%s trade_date=%s",
                    desk_code,
                    trade_date,
                )
    except Exception:
        logger.exception(
            "Failed to write audit record for desk_code=%s trade_date=%s",
            desk_code,
            trade_date,
        )
        raise
    finally:
        if conn is not None:
            conn.close()