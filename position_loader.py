# BOILERPLATE
import logging
import os

import pandas as pd
import psycopg2
import psycopg2.extras

import secret_manager

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — batch size for execute_values inserts; keeps memory and round-trip overhead balanced
_BATCH_SIZE = 1000

# LOGIC — exact column order for INSERT must match VALUES tuple position
_INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — SQL uses ON CONFLICT DO NOTHING for idempotent deduplication on the composite PK
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _build_row_tuple(row) -> tuple:
    # LOGIC — convert a DataFrame row into the positional tuple for execute_values
    return (
        row["trade_id"],
        row["desk_code"],
        row["trade_date"],       # datetime.date — psycopg2 binds natively to DATE
        row["instrument_type"],
        row["notional_amount"],  # Decimal — psycopg2 binds natively to NUMERIC
        row["currency"],
        row["counterparty_id"],
    )


def _get_connection(credentials: dict):
    # BOILERPLATE — build psycopg2 connection from credentials dict
    return psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC — entry point: load all valid rows into demo_schema.trade_positions
    if valid_df.empty:
        logger.info("load_positions: valid_df is empty — nothing to insert")
        return 0

    credentials = secret_manager.get_db_credentials()
    conn = None
    total_inserted = 0

    try:
        conn = _get_connection(credentials)
        logger.info("load_positions: connected to database — beginning batch inserts")

        # LOGIC — convert DataFrame rows to list of tuples in INSERT column order
        all_tuples = [_build_row_tuple(row) for _, row in valid_df.iterrows()]
        total_rows = len(all_tuples)

        # LOGIC — iterate in batches of _BATCH_SIZE
        for batch_start in range(0, total_rows, _BATCH_SIZE):
            batch = all_tuples[batch_start: batch_start + _BATCH_SIZE]
            batch_end = batch_start + len(batch)

            with conn.cursor() as cursor:
                # LOGIC — execute_values issues a single multi-row INSERT per batch
                psycopg2.extras.execute_values(
                    cursor,
                    _INSERT_SQL,
                    batch,
                    template=None,
                    page_size=_BATCH_SIZE,
                )
                # LOGIC — rowcount reflects only rows actually inserted (not skipped by DO NOTHING)
                batch_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0
                total_inserted += batch_inserted

            # LOGIC — commit after each batch for durability; avoid one giant transaction
            conn.commit()

            logger.info(
                "load_positions: batch rows %d-%d — inserted=%d  (cumulative inserted=%d)",
                batch_start + 1,
                batch_end,
                batch_inserted,
                total_inserted,
            )

    except Exception:
        logger.exception("load_positions: unhandled exception during batch insert — rolling back")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("load_positions: rollback failed")
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
                logger.info("load_positions: database connection closed")
            except Exception:
                logger.exception("load_positions: error closing connection")

    logger.info(
        "load_positions: complete — total_rows_attempted=%d  total_inserted=%d  skipped_duplicate=%d",
        total_rows,
        total_inserted,
        total_rows - total_inserted,
    )
    return total_inserted