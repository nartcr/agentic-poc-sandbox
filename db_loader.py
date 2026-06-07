# BOILERPLATE
import logging
from typing import List, Tuple

import pandas as pd
from psycopg2.extras import execute_values

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — exact target table from data contract
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions (
    trade_id,
    desk_code,
    trade_date,
    instrument_type,
    notional_amount,
    currency,
    counterparty_id
)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df: pd.DataFrame, conn) -> int:
    # LOGIC — entry point for bulk idempotent insert
    if valid_df.empty:
        logger.info("load_positions: valid_df is empty, nothing to insert")
        return 0

    rows = _build_insert_tuples(valid_df)
    logger.info("load_positions: preparing to insert up to %d rows", len(rows))

    try:
        with conn.cursor() as cursor:
            # LOGIC — bulk insert; DO NOTHING on conflict satisfies TAC-3
            execute_values(
                cursor,
                _INSERT_SQL,
                rows,
                page_size=1000,
            )
            # LOGIC — rowcount reflects only actually-inserted rows (not DO-NOTHING skips)
            inserted = cursor.rowcount
            conn.commit()

        logger.info(
            "load_positions: committed %d newly inserted rows (skipped %d duplicates)",
            inserted,
            len(rows) - inserted,
        )
        return inserted

    except Exception as exc:
        # LOGIC — always roll back on any DB error before re-raising
        logger.error("load_positions: database error, rolling back. Detail: %s", exc)
        conn.rollback()
        raise


def _build_insert_tuples(df: pd.DataFrame) -> List[Tuple]:
    # LOGIC — build list of tuples in column order matching the INSERT statement
    # notional_amount is stored as str in the DataFrame; cast to float for DB
    tuples = []
    for row in df.itertuples(index=False):
        tuples.append(
            (
                row.trade_id,
                row.desk_code,
                row.trade_date,
                row.instrument_type,
                float(row.notional_amount),  # LOGIC — cast str -> float for NUMERIC column
                row.currency,
                row.counterparty_id,
            )
        )
    return tuples