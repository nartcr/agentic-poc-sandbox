# BOILERPLATE
import logging
import os
from datetime import date, datetime
from typing import Optional

import psycopg2

import secrets_client
from pipeline_exceptions import DatabaseLoadError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows, rows_inserted,
     rows_rejected, error_message, processing_timestamp_et)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

_VALID_STATUSES = {"SUCCESS", "PARTIAL", "FAILED"}


def write_audit_record(
    filename: str,
    desk_code: Optional[str],
    trade_date: Optional[date],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_ts: datetime,
) -> None:
    # LOGIC — validate status before touching the database
    if status not in _VALID_STATUSES:
        raise ValueError(
            f"Invalid audit status '{status}'. Must be one of: {_VALID_STATUSES}"
        )

    # BOILERPLATE — retrieve credentials from Secrets Manager at runtime
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Retrieving DB credentials from Secrets Manager (secret_id=%s)", secret_id)
    try:
        creds = secrets_client.get_secret(secret_id)
    except Exception as exc:
        logger.error("Failed to retrieve DB credentials: %s", exc)
        raise DatabaseLoadError(f"Secrets retrieval failed for audit write: {exc}") from exc

    # BOILERPLATE — establish psycopg2 connection using runtime credentials only
    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
    except psycopg2.Error as exc:
        logger.error("psycopg2 connection failed for audit write: %s", exc)
        raise DatabaseLoadError(f"Database connection failed for audit write: {exc}") from exc

    # LOGIC — insert exactly one audit row per pipeline run
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info(
                    "Writing audit record: filename=%s status=%s total_rows=%d "
                    "rows_inserted=%d rows_rejected=%d",
                    filename,
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                )
                cur.execute(
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
                        processing_ts,
                    ),
                )
                logger.info(
                    "Audit record inserted successfully for filename=%s status=%s",
                    filename,
                    status,
                )
    except psycopg2.Error as exc:
        logger.error("Failed to insert audit record: %s", exc)
        raise DatabaseLoadError(f"Audit record insert failed: {exc}") from exc
    finally:
        # BOILERPLATE — always close the connection
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass