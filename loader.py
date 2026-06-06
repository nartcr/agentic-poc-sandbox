# BOILERPLATE
import logging

import psycopg2
import psycopg2.extras
import pandas as pd

# BOILERPLATE — import secrets module; aliased to avoid shadowing stdlib 'secrets'
import secrets as db_secrets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_INSERT_SQL = """
INSERT INTO rfdh.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _build_records(df: pd.DataFrame) -> list:
    # LOGIC — convert DataFrame rows to list of tuples in column order
    return [
        (
            row["trade_id"],
            row["desk_code"],
            row["trade_date"],
            row["instrument_type"],
            float(row["notional_amount"]),
            row["currency"],
            row["counterparty_id"],
        )
        for _, row in df.iterrows()
    ]


def load_positions(df: pd.DataFrame) -> int:
    # LOGIC — return 0 immediately if there are no rows to insert
    if df.empty:
        logger.info("No valid rows to insert; skipping DB write.")
        return 0

    # BOILERPLATE — fetch credentials at runtime; never cached, supports rotation
    creds = db_secrets.get_db_credentials()

    conn = None
    cursor = None
    try:
        # BOILERPLATE — establish connection using runtime credentials only
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        cursor = conn.cursor()

        # LOGIC — build tuple records from validated DataFrame
        records = _build_records(df)

        # LOGIC — execute batched insert; ON CONFLICT DO NOTHING deduplicates
        psycopg2.extras.execute_values(
            cursor,
            _INSERT_SQL,
            records,
            template=None,
            page_size=1000,
        )

        # LOGIC — rowcount reflects only net-new rows; DO NOTHING rows are excluded
        rows_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0

        conn.commit()

        logger.info(
            "Inserted %d rows into rfdh.trade_positions (attempted %d).",
            rows_inserted,
            len(records),
        )
        return rows_inserted

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("Rollback failed after insert error.")
        logger.exception("Failed to insert rows into rfdh.trade_positions.")
        raise

    finally:
        # BOILERPLATE — always release DB resources
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                logger.warning("Failed to close cursor.")
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close connection.")