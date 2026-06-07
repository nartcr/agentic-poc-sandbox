# BOILERPLATE
import logging
from typing import List, Tuple

import pandas as pd
import psycopg2.extras

logger = logging.getLogger(__name__)

# LOGIC — Target table in the database
_TABLE = "demo_schema.trade_positions"

# LOGIC — Ordered list of columns to insert (matches table DDL, excludes loaded_at which defaults)
_INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _build_row_tuples(valid_df: pd.DataFrame) -> List[Tuple]:
    # LOGIC — Converts DataFrame rows into a list of tuples in _INSERT_COLUMNS order
    return [
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


def _build_conflict_key_tuples(valid_df: pd.DataFrame) -> List[Tuple]:
    # LOGIC — Builds composite key tuples (trade_id, desk_code, trade_date) for COUNT queries
    return [
        (row["trade_id"], row["desk_code"], row["trade_date"])
        for _, row in valid_df.iterrows()
    ]


def _count_existing_rows(cursor, key_tuples: List[Tuple]) -> int:
    # LOGIC — Counts how many of the candidate composite keys already exist in the table
    # Uses a VALUES list joined via INNER JOIN to avoid very long IN-list SQL
    if not key_tuples:
        return 0

    # Build a parameterised query using a VALUES list for the composite keys
    # psycopg2 mogrify-style: pass each triple as a tuple row
    values_template = ",".join(["%s"] * len(key_tuples))
    sql = f"""
        SELECT COUNT(*)
        FROM {_TABLE} tp
        INNER JOIN (VALUES {values_template}) AS v(trade_id, desk_code, trade_date)
          ON tp.trade_id = v.trade_id
         AND tp.desk_code = v.desk_code
         AND tp.trade_date = v.trade_date::date
    """
    cursor.execute(sql, key_tuples)
    result = cursor.fetchone()
    return int(result[0])


def load_positions(valid_df: pd.DataFrame, conn) -> int:
    # LOGIC — Batch-inserts validated rows into demo_schema.trade_positions with idempotent upsert
    if valid_df.empty:
        logger.info("valid_df is empty — no rows to insert")
        return 0

    row_tuples = _build_row_tuples(valid_df)
    key_tuples = _build_conflict_key_tuples(valid_df)

    logger.info(
        "Preparing to insert %d rows into %s",
        len(row_tuples),
        _TABLE,
    )

    columns_sql = ", ".join(_INSERT_COLUMNS)
    insert_sql = f"""
        INSERT INTO {_TABLE} ({columns_sql})
        VALUES %s
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
    """

    with conn.cursor() as cursor:
        # LOGIC — Count pre-existing rows before insert to compute true delta
        pre_count = _count_existing_rows(cursor, key_tuples)
        logger.info("Pre-insert existing row count for this key set: %d", pre_count)

        # LOGIC — Execute batch insert using execute_values for performance
        psycopg2.extras.execute_values(
            cursor,
            insert_sql,
            row_tuples,
            template=None,
            page_size=len(row_tuples),
            fetch=False,
        )

        # LOGIC — Count rows now present after insert to compute delta
        post_count = _count_existing_rows(cursor, key_tuples)
        logger.info("Post-insert existing row count for this key set: %d", post_count)

    rows_inserted = post_count - pre_count
    logger.info(
        "Rows actually inserted (delta): %d  (pre=%d, post=%d)",
        rows_inserted,
        pre_count,
        post_count,
    )
    return rows_inserted