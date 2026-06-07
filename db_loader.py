# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

from secret_client import get_db_credentials

logger = logging.getLogger(__name__)

# LOGIC — exact table reference from infrastructure config
_TABLE = "demo_schema.trade_positions"

# LOGIC — exact INSERT SQL from approved design; ON CONFLICT ensures idempotency
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — SQL to count rows already present for a given set of composite keys
_COUNT_EXISTING_SQL = """
SELECT COUNT(*) FROM demo_schema.trade_positions
WHERE (trade_id, desk_code, trade_date) = ANY(%s)
"""

_ET_TZ = pytz.timezone("America/Toronto")


def _build_row_tuples(valid_df, loaded_at: datetime) -> list[tuple]:
    # LOGIC — convert DataFrame rows to positional tuples matching INSERT column order
    rows = []
    for _, row in valid_df.iterrows():
        rows.append((
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],           # datetime.date — psycopg2 maps to DATE
            str(row["instrument_type"]),
            row["notional_amount"],      # Decimal — psycopg2 maps to NUMERIC
            str(row["currency"]),
            str(row["counterparty_id"]),
            loaded_at,                   # timezone-aware datetime — maps to TIMESTAMPTZ
        ))
    return rows


def _count_existing_keys(cursor, composite_keys: list[tuple]) -> int:
    # LOGIC — count how many of the candidate composite keys already exist in the table
    # psycopg2 accepts a list of tuples for = ANY(%s) with adapt
    if not composite_keys:
        return 0
    cursor.execute(_COUNT_EXISTING_SQL, (composite_keys,))
    result = cursor.fetchone()
    return int(result[0]) if result else 0


def load_positions(valid_df) -> int:
    # LOGIC — main entry point; inserts valid rows and returns count of actually inserted rows
    import pandas as pd

    if valid_df is None or (hasattr(valid_df, "__len__") and len(valid_df) == 0):
        logger.info("No valid rows to insert; skipping DB load.")
        return 0

    # BOILERPLATE — get DB credentials from Secrets Manager at runtime
    creds = get_db_credentials()

    # LOGIC — ET-aware loaded_at timestamp, set once for the entire batch
    loaded_at = datetime.now(_ET_TZ)

    row_tuples = _build_row_tuples(valid_df, loaded_at)
    total_candidates = len(row_tuples)

    if total_candidates == 0:
        logger.info("DataFrame produced zero row tuples; nothing to insert.")
        return 0

    # LOGIC — build composite key list for pre-insert existence count
    composite_keys = [
        (str(row["trade_id"]), str(row["desk_code"]), row["trade_date"])
        for _, row in valid_df.iterrows()
    ]

    conn = None
    cursor = None
    try:
        # BOILERPLATE — establish psycopg2 connection from Secrets Manager credentials
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        conn.autocommit = False
        cursor = conn.cursor()

        # LOGIC — count pre-existing rows to compute accurate inserted count
        pre_existing = _count_existing_keys(cursor, composite_keys)
        logger.info(
            "Inserting %d candidate rows; %d already exist in %s.",
            total_candidates,
            pre_existing,
            _TABLE,
        )

        # LOGIC — batch insert entire DataFrame in a single transaction
        cursor.executemany(_INSERT_SQL, row_tuples)
        conn.commit()

        # LOGIC — rows_inserted = candidates that were NOT already present
        rows_inserted = total_candidates - pre_existing
        # Guard against negative count if pre_existing count is stale (edge case)
        rows_inserted = max(rows_inserted, 0)

        logger.info(
            "Insert complete: %d inserted, %d skipped (ON CONFLICT DO NOTHING).",
            rows_inserted,
            total_candidates - rows_inserted,
        )
        return rows_inserted

    except Exception as exc:
        logger.error("DB insert failed; rolling back transaction. Error: %s", exc)
        if conn is not None:
            try:
                conn.rollback()
                logger.info("Transaction rolled back successfully.")
            except Exception as rb_exc:
                logger.error("Rollback also failed: %s", rb_exc)
        raise

    finally:
        # BOILERPLATE — always close cursor and connection
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass