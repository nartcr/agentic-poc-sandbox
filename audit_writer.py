# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import psycopg2
import pytz

import db_connector
from ingestion_exceptions import DBConnectionError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET_TZ = pytz.timezone("America/Toronto")

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
    """
    Insert one row into demo_schema.pipeline_audit for every pipeline invocation.
    No ON CONFLICT clause — every call produces a new row for full audit history.
    Satisfies: BAC-4, BAC-7, BAC-8.
    """
    # LOGIC — build ET-aware timestamp at the moment of audit write
    processing_timestamp_et = datetime.now(_ET_TZ)

    # LOGIC — cast trade_date string to datetime.date for the DATE column; None passes as NULL
    trade_date_value: "datetime.date | None" = None
    if trade_date is not None:
        try:
            trade_date_value = datetime.strptime(trade_date, "%Y-%m-%d").date()
        except ValueError:
            # LOGIC — if unparseable, store NULL rather than hard-failing the audit write
            logger.warning(
                "audit_writer: trade_date %r could not be parsed as YYYY-MM-DD; storing NULL",
                trade_date,
            )
            trade_date_value = None

    # BOILERPLATE — INSERT SQL uses exact column names from data contract
    insert_sql = """
        INSERT INTO demo_schema.pipeline_audit
            (filename, desk_code, trade_date, status,
             total_rows, rows_inserted, rows_rejected,
             error_message, processing_timestamp_et)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = None
    try:
        # BOILERPLATE — obtain connection; caller does not share a connection with audit_writer
        conn = db_connector.get_connection()
        with conn.cursor() as cur:
            # LOGIC — bind all nine positional parameters in column order
            cur.execute(
                insert_sql,
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
        conn.commit()
        logger.info(
            "audit_writer: wrote audit record | filename=%s status=%s "
            "total_rows=%d rows_inserted=%d rows_rejected=%d timestamp_et=%s",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
            processing_timestamp_et.isoformat(),
        )
    except DBConnectionError:
        # LOGIC — propagate connection errors so the handler can log them without masking
        logger.error(
            "audit_writer: DB connection failed while writing audit record for %s", filename
        )
        raise
    except psycopg2.Error as exc:
        logger.error(
            "audit_writer: psycopg2 error writing audit record for %s: %s",
            filename,
            exc,
        )
        if conn is not None:
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
        raise
    finally:
        # BOILERPLATE — always close the connection; Lambda is stateless
        if conn is not None:
            try:
                conn.close()
            except psycopg2.Error:
                pass