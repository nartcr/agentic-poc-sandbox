# BOILERPLATE
import logging
import os
from datetime import datetime
from decimal import Decimal

import pandas as pd
import psycopg2
import psycopg2.extras

import secret_client
from pipeline_exceptions import DatabaseLoadError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — batch size for execute_values inserts
_BATCH_SIZE = 1000


def get_db_connection() -> psycopg2.extensions.connection:
    # LOGIC — retrieve credentials from Secrets Manager; never hardcode
    secret = secret_client.get_secret()
    try:
        conn = psycopg2.connect(
            host=secret["host"],
            port=int(secret["port"]),
            dbname=secret["dbname"],
            user=secret["username"],
            password=secret["password"],
            connect_timeout=10,
        )
        logger.info("Database connection established.")
        return conn
    except psycopg2.Error as exc:
        logger.error("Failed to establish database connection: %s", exc)
        raise DatabaseLoadError(f"Could not connect to database: {exc}") from exc


def load_positions(valid_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> int:
    # LOGIC — idempotent batch insert using ON CONFLICT DO NOTHING
    if valid_df.empty:
        logger.info("valid_df is empty; no rows to insert.")
        return 0

    # LOGIC — use pre/post count delta to get accurate inserted row count
    # (cursor.rowcount with execute_values reflects affected rows but behaviour
    # can vary; count delta is deterministic)
    desk_codes = valid_df["desk_code"].unique().tolist()
    trade_dates = valid_df["trade_date"].unique().tolist()

    count_before = _count_existing_rows(conn, desk_codes, trade_dates)

    # LOGIC — build list of tuples in the exact column order of the INSERT
    rows = [
        (
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],          # datetime.date after validation cast
            str(row["instrument_type"]),
            row["notional_amount"],     # Decimal after validation cast
            str(row["currency"]),
            str(row["counterparty_id"]),
        )
        for _, row in valid_df.iterrows()
    ]

    # LOGIC — exact INSERT statement from approved design
    insert_sql = """
        INSERT INTO demo_schema.trade_positions
            (trade_id, desk_code, trade_date, instrument_type,
             notional_amount, currency, counterparty_id)
        VALUES %s
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
    """

    try:
        with conn.cursor() as cur:
            # LOGIC — process in batches of 1,000 rows (TAC-6)
            for batch_start in range(0, len(rows), _BATCH_SIZE):
                batch = rows[batch_start: batch_start + _BATCH_SIZE]
                psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=_BATCH_SIZE)
                logger.info(
                    "Inserted batch rows %d–%d (batch size: %d).",
                    batch_start + 1,
                    batch_start + len(batch),
                    len(batch),
                )
        conn.commit()
    except psycopg2.Error as exc:
        conn.rollback()
        logger.error("Database insert failed; transaction rolled back: %s", exc)
        raise DatabaseLoadError(f"load_positions failed: {exc}") from exc

    count_after = _count_existing_rows(conn, desk_codes, trade_dates)
    rows_inserted = count_after - count_before
    logger.info(
        "load_positions complete. Submitted: %d, Actually inserted: %d, Skipped (duplicates): %d.",
        len(rows),
        rows_inserted,
        len(rows) - rows_inserted,
    )
    return rows_inserted


def _count_existing_rows(
    conn: psycopg2.extensions.connection,
    desk_codes: list,
    trade_dates: list,
) -> int:
    # LOGIC — count rows matching any of the desk_code + trade_date combinations
    # present in this batch; used for pre/post delta calculation
    if not desk_codes or not trade_dates:
        return 0
    sql = """
        SELECT COUNT(*)
        FROM demo_schema.trade_positions
        WHERE desk_code = ANY(%s)
          AND trade_date = ANY(%s)
        ORDER BY 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (desk_codes, trade_dates))
            result = cur.fetchone()
            return int(result[0]) if result else 0
    except psycopg2.Error as exc:
        logger.error("Count query failed: %s", exc)
        raise DatabaseLoadError(f"_count_existing_rows failed: {exc}") from exc


def write_audit_record(
    conn: psycopg2.extensions.connection,
    filename: str,
    desk_code: str,
    trade_date: str,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_timestamp_et: datetime,
) -> int:
    # LOGIC — insert a new pipeline_audit row and return the generated audit_id
    insert_sql = """
        INSERT INTO demo_schema.pipeline_audit
            (filename, desk_code, trade_date, status,
             total_rows, rows_inserted, rows_rejected,
             error_message, processing_timestamp_et)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING audit_id
    """
    # LOGIC — trade_date may be None if filename parsing failed
    trade_date_value = None
    if trade_date:
        try:
            from datetime import date as _date
            trade_date_value = _date.fromisoformat(trade_date)
        except ValueError:
            trade_date_value = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    filename,
                    desk_code if desk_code else None,
                    trade_date_value,
                    status,
                    total_rows,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    processing_timestamp_et,
                ),
            )
            row = cur.fetchone()
            audit_id = int(row[0])
        conn.commit()
        logger.info("Audit record created: audit_id=%d, status=%s.", audit_id, status)
        return audit_id
    except psycopg2.Error as exc:
        conn.rollback()
        logger.error("write_audit_record failed: %s", exc)
        raise DatabaseLoadError(f"write_audit_record failed: {exc}") from exc


def update_audit_record(
    conn: psycopg2.extensions.connection,
    audit_id: int,
    status: str,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
) -> None:
    # LOGIC — update an existing pipeline_audit row by audit_id
    update_sql = """
        UPDATE demo_schema.pipeline_audit
        SET status         = %s,
            rows_inserted  = %s,
            rows_rejected  = %s,
            error_message  = %s
        WHERE audit_id = %s
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                update_sql,
                (
                    status,
                    rows_inserted,
                    rows_rejected,
                    error_message,
                    audit_id,
                ),
            )
        conn.commit()
        logger.info(
            "Audit record updated: audit_id=%d, status=%s, rows_inserted=%d, rows_rejected=%d.",
            audit_id,
            status,
            rows_inserted,
            rows_rejected,
        )
    except psycopg2.Error as exc:
        conn.rollback()
        logger.error("update_audit_record failed: %s", exc)
        raise DatabaseLoadError(f"update_audit_record failed: {exc}") from exc