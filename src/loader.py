# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET = pytz.timezone("America/Toronto")

_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id, loaded_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(conn, valid_df) -> int:
    """
    Insert validated rows into demo_schema.trade_positions.

    Returns the number of rows actually inserted (rows skipped by
    ON CONFLICT DO NOTHING are excluded from the count).

    Commits on success; rolls back and re-raises on any exception.
    """
    # LOGIC — determine load timestamp once for the whole batch (ET, timezone-aware)
    loaded_at = datetime.now(_ET)

    # LOGIC — build list of tuples matching the INSERT parameter order
    rows = [
        (
            row["trade_id"],
            row["desk_code"],
            row["trade_date"],
            row["instrument_type"],
            row["notional_amount"],
            row["currency"],
            row["counterparty_id"],
            loaded_at,
        )
        for _, row in valid_df.iterrows()
    ]

    if not rows:
        logger.info("load_positions: no valid rows to insert; skipping DB write")
        return 0

    cursor = conn.cursor()
    try:
        # LOGIC — single executemany call for throughput (TAC-6)
        cursor.executemany(_INSERT_SQL, rows)
        rows_inserted = cursor.rowcount
        conn.commit()
        logger.info(
            "load_positions: attempted=%d inserted=%d",
            len(rows),
            rows_inserted,
        )
        return rows_inserted
    except Exception:
        conn.rollback()
        logger.exception("load_positions: error during insert; transaction rolled back")
        raise
    finally:
        cursor.close()