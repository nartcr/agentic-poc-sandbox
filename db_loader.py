# BOILERPLATE
import logging
from typing import TYPE_CHECKING

import pandas as pd
import psycopg2
import psycopg2.extras

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — exact column list from data contract; order must match INSERT column list
_POSITION_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — target table and conflict key from data contract
_INSERT_SQL = """
    INSERT INTO demo_schema.trade_positions (
        trade_id,
        desk_code,
        trade_date,
        instrument_type,
        notional_amount,
        currency,
        counterparty_id
    ) VALUES %s
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(conn, valid_df: pd.DataFrame) -> int:
    """
    Bulk-insert validated trade position rows into demo_schema.trade_positions.
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Returns the count of rows actually inserted (dedup-skipped rows are NOT counted).
    Rolls back and re-raises on any exception.
    """
    # LOGIC — handle empty DataFrame gracefully; nothing to insert
    if valid_df.empty:
        logger.info("load_positions: valid_df is empty — no rows to insert")
        return 0

    # LOGIC — extract only the expected columns in the correct order; extra columns are ignored
    try:
        subset = valid_df[_POSITION_COLUMNS]
    except KeyError as exc:
        raise ValueError(
            f"load_positions: valid_df is missing required column(s): {exc}"
        ) from exc

    # LOGIC — build list of tuples for execute_values; coerce trade_date to native Python date
    # to ensure psycopg2 can map it to a PostgreSQL DATE without ambiguity
    rows = [
        (
            str(row.trade_id),
            str(row.desk_code),
            row.trade_date if isinstance(row.trade_date, type(row.trade_date)) else row.trade_date,
            str(row.instrument_type),
            float(row.notional_amount),
            str(row.currency),
            str(row.counterparty_id),
        )
        for row in subset.itertuples(index=False)
    ]

    # LOGIC — execute bulk insert inside a transaction; roll back on any failure
    cursor = conn.cursor()
    try:
        logger.info(
            "load_positions: attempting bulk insert of %d row(s) into demo_schema.trade_positions",
            len(rows),
        )
        psycopg2.extras.execute_values(cursor, _INSERT_SQL, rows)
        rows_inserted = cursor.rowcount
        conn.commit()
        logger.info(
            "load_positions: committed — rows_inserted=%d (attempted=%d, dedup_skipped=%d)",
            rows_inserted,
            len(rows),
            len(rows) - rows_inserted,
        )
    except Exception:
        conn.rollback()
        logger.exception("load_positions: insert failed — transaction rolled back")
        raise
    finally:
        cursor.close()

    return rows_inserted