# BOILERPLATE
import logging
import os
from decimal import Decimal, InvalidOperation

import psycopg2
import pandas as pd

import secrets_client
from pipeline_exceptions import DatabaseLoadError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING;
"""


def _get_connection(secret: dict):
    # BOILERPLATE — builds psycopg2 connection from secret dict; no hardcoded credentials
    return psycopg2.connect(
        host=secret["host"],
        port=int(secret["port"]),
        dbname=secret["dbname"],
        user=secret["username"],
        password=secret["password"],
    )


def _row_to_tuple(row: pd.Series) -> tuple:
    # LOGIC — converts a DataFrame row to a parameterised tuple for the INSERT
    try:
        notional = Decimal(str(row["notional_amount"]))
    except InvalidOperation as exc:
        raise DatabaseLoadError(
            f"Cannot convert notional_amount to Decimal for trade_id={row.get('trade_id')}: {exc}"
        ) from exc

    return (
        str(row["trade_id"]),
        str(row["desk_code"]),
        str(row["trade_date"]),
        str(row["instrument_type"]),
        notional,
        str(row["currency"]),
        str(row["counterparty_id"]),
    )


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC — idempotent batch insert; returns actual count of rows inserted (duplicates excluded)
    if valid_df.empty:
        logger.info("valid_df is empty — nothing to insert")
        return 0

    # BOILERPLATE — retrieve credentials at runtime from Secrets Manager
    secret_id = os.environ["DB_SECRET_ID"]
    try:
        secret = secrets_client.get_secret(secret_id)
    except Exception as exc:
        raise DatabaseLoadError(f"Failed to retrieve DB secret '{secret_id}': {exc}") from exc

    rows = [_row_to_tuple(row) for _, row in valid_df.iterrows()]

    conn = None
    try:
        conn = _get_connection(secret)
        conn.autocommit = False
        cursor = conn.cursor()

        # LOGIC — execute row-by-row inside a single transaction so we can accumulate
        # rowcount accurately. psycopg2 executemany() resets rowcount to the last
        # statement's affected rows, so individual execute() calls are needed for an
        # accurate cumulative count with ON CONFLICT DO NOTHING.
        rows_inserted = 0
        for row_tuple in rows:
            cursor.execute(_INSERT_SQL, row_tuple)
            # rowcount is 1 if the row was inserted, 0 if skipped by ON CONFLICT
            if cursor.rowcount == 1:
                rows_inserted += 1

        conn.commit()
        logger.info(
            "Inserted %d rows into demo_schema.trade_positions (%d skipped as duplicates)",
            rows_inserted,
            len(rows) - rows_inserted,
        )
        return rows_inserted

    except psycopg2.Error as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise DatabaseLoadError(f"Database error during load_positions: {exc}") from exc

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass