# BOILERPLATE
import logging
import os
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# LOGIC — schema from infrastructure config, never hardcoded as a magic string
_SCHEMA = os.environ.get("DB_SCHEMA", "demo_schema")


def load_positions(valid_df: pd.DataFrame, conn) -> int:
    # LOGIC — batch insert with ON CONFLICT DO NOTHING for idempotency (TAC-3)
    if valid_df.empty:
        logger.info("load_positions: valid_df is empty, nothing to insert")
        return 0

    table = f"{_SCHEMA}.trade_positions"

    # LOGIC — build list of tuples in exact column order matching the INSERT
    rows = [
        (
            row["trade_id"],
            row["desk_code"],
            row["trade_date"],
            row["instrument_type"],
            row["notional_amount"],
            row["currency"],
            row["counterparty_id"],
        )
        for _, row in valid_df.iterrows()
    ]

    # LOGIC — count rows before insert to compute actual inserted count
    # execute_values rowcount is unreliable with ON CONFLICT DO NOTHING across drivers;
    # use pre/post COUNT to get the true inserted count
    with conn.cursor() as cur:
        # LOGIC — get count of already-existing rows matching our composite keys
        # Build a VALUES list to query only the keys we are about to insert
        key_tuples = [(r[0], r[1], r[2]) for r in rows]
        psycopg2.extras.execute_values(
            cur,
            "SELECT COUNT(*) FROM {table} WHERE (trade_id, desk_code, trade_date) IN %s".format(
                table=table
            ),
            [tuple(key_tuples)],
            template=None,
            page_size=len(key_tuples),
        )
        pre_existing_count = cur.fetchone()[0]

    with conn.cursor() as cur:
        # LOGIC — single-round-trip batch insert (TAC-6)
        insert_sql = (
            f"INSERT INTO {table} "
            "(trade_id, desk_code, trade_date, instrument_type, "
            "notional_amount, currency, counterparty_id) "
            "VALUES %s "
            "ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING"
        )
        psycopg2.extras.execute_values(
            cur,
            insert_sql,
            rows,
            template=None,
            page_size=1000,
        )
        conn.commit()
        logger.info(
            "load_positions: execute_values completed, rowcount=%s", cur.rowcount
        )

    # LOGIC — rows_inserted = attempted rows minus those that already existed
    rows_inserted = len(rows) - pre_existing_count
    logger.info(
        "load_positions: %d rows attempted, %d pre-existing, %d inserted",
        len(rows),
        pre_existing_count,
        rows_inserted,
    )
    return rows_inserted


def write_audit_record(
    conn,
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_timestamp_et: datetime,
) -> None:
    # LOGIC — exact column names from infrastructure config YAML for pipeline_audit table
    table = f"{_SCHEMA}.pipeline_audit"

    sql = (
        f"INSERT INTO {table} "
        "(filename, desk_code, trade_date, status, total_rows, rows_inserted, "
        "rows_rejected, error_message, processing_timestamp_et) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    params = (
        filename,
        desk_code,
        trade_date,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
        error_message,
        processing_timestamp_et,
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()

    logger.info(
        "write_audit_record: wrote audit row for filename=%s status=%s",
        filename,
        status,
    )