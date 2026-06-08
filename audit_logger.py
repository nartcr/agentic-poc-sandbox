# BOILERPLATE
import json
import logging
import os
from datetime import datetime, date

import psycopg2
import pytz

import secrets_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — Eastern Time zone constant
_ET = pytz.timezone("America/Toronto")


def write_audit(
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
) -> None:
    # LOGIC — capture ET timestamp at call time before any DB I/O
    processing_timestamp_et = datetime.now(_ET)

    # LOGIC — coerce trade_date string to datetime.date for the DATE column;
    # the column is nullable so None is passed through unchanged
    trade_date_value = None
    if trade_date is not None:
        if isinstance(trade_date, date):
            trade_date_value = trade_date
        else:
            try:
                trade_date_value = datetime.strptime(str(trade_date), "%Y-%m-%d").date()
            except ValueError:
                # LOGIC — unparseable date: pass None so audit row still lands
                logger.warning(
                    "audit_logger: could not parse trade_date=%r as YYYY-MM-DD; "
                    "inserting NULL for trade_date column",
                    trade_date,
                )
                trade_date_value = None

    conn = None
    try:
        # BOILERPLATE — retrieve credentials at runtime; never hardcoded
        creds = secrets_client.get_db_credentials()
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        with conn.cursor() as cur:
            # LOGIC — insert one immutable audit record per pipeline run;
            # audit_id is BIGSERIAL so it is not supplied
            cur.execute(
                """
                INSERT INTO demo_schema.pipeline_audit
                    (filename, desk_code, trade_date, status, total_rows,
                     rows_inserted, rows_rejected, error_message,
                     processing_timestamp_et)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    filename,
                    desk_code,
                    trade_date_value,
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    processing_timestamp_et,
                ),
            )
        # LOGIC — commit immediately; each audit row is an independent record
        conn.commit()
        logger.info(
            "audit_logger: wrote audit record filename=%r status=%r "
            "total_rows=%d rows_inserted=%d rows_rejected=%d",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except Exception:
        # LOGIC — audit write failures must NOT mask the primary pipeline error;
        # log and swallow so the caller's error propagates unobstructed
        logger.error(
            "audit_logger: failed to write audit record for filename=%r; "
            "pipeline processing continues",
            filename,
            exc_info=True,
        )
    finally:
        # BOILERPLATE — always release the DB connection
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning(
                    "audit_logger: exception while closing DB connection",
                    exc_info=True,
                )