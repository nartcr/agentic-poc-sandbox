# BOILERPLATE
import logging
import os
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
import pandas as pd

from secret_loader import get_db_secret
from timestamp_helper import now_et

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — schema and table constants read from infra config
_SCHEMA = os.environ.get("DB_SCHEMA", "demo_schema")
_TABLE_POSITIONS = f"{_SCHEMA}.trade_positions"
_TABLE_AUDIT = f"{_SCHEMA}.pipeline_audit"


def _get_connection():
    # BOILERPLATE — build connection from secrets, no hardcoded credentials
    secret = get_db_secret()
    conn = psycopg2.connect(
        host=secret["host"],
        port=int(secret["port"]),
        user=secret["username"],
        password=secret["password"],
        dbname=secret["dbname"],
    )
    return conn


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC — bulk insert validated rows into demo_schema.trade_positions
    # Returns count of rows actually inserted (duplicates excluded via ON CONFLICT DO NOTHING)
    if valid_df.empty:
        logger.info("load_positions called with empty DataFrame; skipping insert.")
        return 0

    # LOGIC — build list of tuples in exact column order matching INSERT statement
    columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]

    rows = [
        (
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],          # already validated as parseable date string
            str(row["instrument_type"]),
            row["notional_amount"],      # already validated as numeric
            str(row["currency"]),
            str(row["counterparty_id"]),
        )
        for _, row in valid_df.iterrows()
    ]

    # LOGIC — exact SQL pattern from design spec; ON CONFLICT ensures idempotency (TAC-3)
    insert_sql = f"""
        INSERT INTO {_TABLE_POSITIONS}
            (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
        VALUES %s
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
    """

    conn = None
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            # LOGIC — execute_values for bulk performance per TAC-6
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=1000)
            rows_inserted = cur.rowcount
            # LOGIC — rowcount after execute_values with DO NOTHING reflects actual inserts
            # psycopg2 returns -1 for execute_values in some versions; fall back to query
            if rows_inserted < 0:
                rows_inserted = _count_inserted(cur, rows)
        conn.commit()
        logger.info(
            "load_positions: inserted %d rows into %s (submitted %d rows).",
            rows_inserted,
            _TABLE_POSITIONS,
            len(rows),
        )
        return rows_inserted
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.exception("load_positions: transaction rolled back due to error.")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _count_inserted(cur, rows: list) -> int:
    # LOGIC — fallback: when rowcount is -1, derive inserted count by checking which
    # (trade_id, desk_code, trade_date) tuples now exist; used only when psycopg2
    # does not return a reliable rowcount for execute_values.
    # This is a best-effort count; the data has already been committed.
    if not rows:
        return 0
    keys = [(r[0], r[1], r[2]) for r in rows]
    cur.execute(
        f"""
        SELECT COUNT(*) FROM {_TABLE_POSITIONS}
        WHERE (trade_id, desk_code, trade_date) IN %s
        ORDER BY 1
        """,
        (tuple(keys),),
    )
    result = cur.fetchone()
    return int(result[0]) if result else 0


def write_audit_record(
    filename: str,
    desk_code: Optional[str],
    trade_date: Optional[str],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_timestamp_et: datetime,
) -> None:
    # LOGIC — writes one row to demo_schema.pipeline_audit per pipeline execution
    # Exact column names from YAML infra config; never invent aliases

    # LOGIC — trade_date must be a date type or None; coerce string to date if present
    trade_date_val: Optional[str] = trade_date  # psycopg2 accepts 'YYYY-MM-DD' strings for DATE

    insert_sql = f"""
        INSERT INTO {_TABLE_AUDIT}
            (filename, desk_code, trade_date, status, total_rows, rows_inserted,
             rows_rejected, error_message, processing_timestamp_et)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    conn = None
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    filename,
                    desk_code,
                    trade_date_val,
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
            "write_audit_record: wrote audit row for filename=%s status=%s.",
            filename,
            status,
        )
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.exception("write_audit_record: failed to write audit row for filename=%s.", filename)
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass