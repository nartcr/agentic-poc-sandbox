# BOILERPLATE
import logging

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# LOGIC — exact column order matching demo_schema.trade_positions (excluding loaded_at which defaults server-side)
_INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — target table in the configured schema
_TABLE = "demo_schema.trade_positions"

# LOGIC — conflict target matches the composite primary key
_INSERT_SQL = f"""
    INSERT INTO {_TABLE}
        (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
    VALUES %s
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> int:
    """
    Batch-insert validated position rows into demo_schema.trade_positions.
    Uses execute_values for performance (TAC-6).
    ON CONFLICT DO NOTHING ensures idempotency (TAC-3).
    Returns the count of rows actually inserted (excluding duplicates skipped).
    """
    # LOGIC — nothing to insert; return early without touching the DB
    if valid_df.empty:
        logger.info("load_positions called with empty DataFrame; nothing to insert.")
        return 0

    # LOGIC — build list of tuples in exact column order for execute_values
    rows = [
        tuple(row[col] for col in _INSERT_COLUMNS)
        for _, row in valid_df.iterrows()
    ]

    total_attempted = len(rows)
    logger.info("Attempting to insert %d rows into %s", total_attempted, _TABLE)

    with conn.cursor() as cursor:
        # LOGIC — batch insert; page_size tuned for throughput on large files
        psycopg2.extras.execute_values(
            cursor,
            _INSERT_SQL,
            rows,
            page_size=1000,
        )

        raw_rowcount = cursor.rowcount

    # LOGIC — commit the transaction; caller does not manage transaction lifecycle
    conn.commit()

    # LOGIC — rowcount from execute_values reflects actual inserted rows (ON CONFLICT skips are excluded)
    # Some psycopg2 versions return -1 for execute_values; treat conservatively
    if raw_rowcount == -1:
        logger.warning(
            "cursor.rowcount returned -1 after execute_values; "
            "cannot determine exact inserted count — defaulting to total_attempted (%d). "
            "Duplicate detection may be inaccurate for this run.",
            total_attempted,
        )
        rows_inserted = total_attempted
    else:
        rows_inserted = raw_rowcount

    rows_skipped = total_attempted - rows_inserted
    logger.info(
        "Insert complete: %d rows inserted, %d rows skipped (duplicates)",
        rows_inserted,
        rows_skipped,
    )

    return rows_inserted