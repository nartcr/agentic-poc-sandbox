# BOILERPLATE
import logging
from decimal import Decimal

import psycopg2
import psycopg2.extras

import pandas as pd

import secrets_client  # BOILERPLATE — sibling module; no credentials in this file

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — exact SQL matching the data contract table and conflict key
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
  (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _get_connection(credentials: dict):
    # BOILERPLATE — open a psycopg2 connection from credential dict; no literals
    return psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )


def _row_to_params(row: pd.Series) -> tuple:
    # LOGIC — convert a DataFrame row into the positional parameter tuple for the INSERT
    # notional_amount is already Decimal from row_validator; psycopg2 handles Decimal natively
    return (
        str(row["trade_id"]),
        str(row["desk_code"]),
        row["trade_date"],           # datetime.date — psycopg2 maps to DATE
        str(row["instrument_type"]),
        row["notional_amount"],      # Decimal — psycopg2 maps to NUMERIC
        str(row["currency"]),
        str(row["counterparty_id"]),
    )


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC — early return when there is nothing to insert; no DB connection opened
    if valid_df is None or valid_df.empty:
        logger.info("load_positions: valid_df is empty; skipping DB insert")
        return 0

    credentials = secrets_client.get_db_credentials()  # LOGIC — runtime credentials only

    conn = None
    try:
        conn = _get_connection(credentials)
        rows_inserted = 0

        with conn.cursor() as cursor:
            # LOGIC — iterate row-by-row so cursor.rowcount is reliable per statement:
            #   1 = the row was inserted, 0 = the row was skipped (conflict)
            for _, row in valid_df.iterrows():
                params = _row_to_params(row)
                cursor.execute(_INSERT_SQL, params)
                rows_inserted += cursor.rowcount  # LOGIC — 1 if inserted, 0 if skipped

        conn.commit()
        logger.info(
            "load_positions: attempted=%d inserted=%d skipped=%d",
            len(valid_df),
            rows_inserted,
            len(valid_df) - rows_inserted,
        )
        return rows_inserted

    except Exception:
        logger.exception("load_positions: unhandled exception; rolling back transaction")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("load_positions: rollback failed")
        raise

    finally:
        # BOILERPLATE — always close the connection
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.exception("load_positions: failed to close DB connection")