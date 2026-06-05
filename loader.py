# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
import pandas as pd
import pytz

from exceptions import LoadError
import config

logger = logging.getLogger(__name__)

# LOGIC — target table and upsert SQL
_INSERT_SQL = """
INSERT INTO app.daily_trades
  (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at, source_file)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

_COLUMN_ORDER = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "loaded_at",
    "source_file",
]


def load_trades(valid_df: pd.DataFrame, source_file: str, credentials: dict) -> int:
    """
    Insert validated trade rows into app.daily_trades using an idempotent upsert.

    Returns the number of rows actually inserted.
    Raises LoadError on any database exception.
    """
    # LOGIC — handle empty DataFrame early
    if valid_df.empty:
        logger.info("No valid rows to load; skipping database insert.")
        return 0

    # LOGIC — compute loaded_at in ET
    loaded_at = datetime.now(pytz.timezone("America/Toronto"))

    # LOGIC — add metadata columns
    insert_df = valid_df.copy()
    insert_df["loaded_at"] = loaded_at
    insert_df["source_file"] = source_file

    # LOGIC — build list of tuples in deterministic column order
    rows = [
        tuple(row[col] for col in _COLUMN_ORDER)
        for _, row in insert_df.iterrows()
    ]

    conn = None
    try:
        # BOILERPLATE — build psycopg2 connection
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            dbname=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
            options="-c search_path=app",
        )
        cursor = conn.cursor()

        # LOGIC — batch upsert
        psycopg2.extras.execute_values(cursor, _INSERT_SQL, rows)
        rows_inserted = cursor.rowcount
        conn.commit()

        logger.info(
            "Loaded %d rows into app.daily_trades from '%s'.",
            rows_inserted,
            source_file,
        )
        return rows_inserted

    except Exception as exc:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise LoadError(f"Database insert failed: {type(exc).__name__}: {exc}") from exc

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass