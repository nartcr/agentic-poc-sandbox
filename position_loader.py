# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — constants
_BATCH_SIZE = 1_000
_ET = pytz.timezone("America/Toronto")
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id, loaded_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _open_connection(credentials: dict):
    """Open and return a psycopg2 connection from a credentials dict."""
    # BOILERPLATE
    return psycopg2.connect(
        host=credentials["host"],
        port=credentials["port"],
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )


def load_positions(valid_df, credentials: dict) -> int:
    """
    Bulk-insert validated trade position rows into demo_schema.trade_positions.

    Uses execute_values in batches of 1,000 rows with ON CONFLICT DO NOTHING
    for idempotency.  Commits once after all batches succeed; rolls back on
    any exception.  Returns the total number of rows actually inserted.
    """
    # LOGIC — capture ET timestamp once for the entire load operation
    loaded_at = datetime.now(_ET)

    # LOGIC — build list of row tuples in column order matching the INSERT statement
    rows = [
        (
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],           # datetime.date, accepted by psycopg2
            str(row["instrument_type"]),
            float(row["notional_amount"]),
            str(row["currency"]),
            str(row["counterparty_id"]),
            loaded_at,
        )
        for _, row in valid_df.iterrows()
    ]

    total_rows = len(rows)
    logger.info(
        "Beginning load of %d valid rows into demo_schema.trade_positions.", total_rows
    )

    if total_rows == 0:
        logger.info("No valid rows to load; returning 0.")
        return 0

    connection = _open_connection(credentials)
    try:
        cursor = connection.cursor()
        rows_inserted = 0

        # LOGIC — iterate in batches of BATCH_SIZE
        for batch_start in range(0, total_rows, _BATCH_SIZE):
            batch = rows[batch_start : batch_start + _BATCH_SIZE]
            psycopg2.extras.execute_values(
                cursor,
                _INSERT_SQL,
                batch,
                template=None,
                page_size=_BATCH_SIZE,
            )
            # LOGIC — rowcount reflects rows actually inserted (DO NOTHING rows = 0)
            batch_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0
            rows_inserted += batch_inserted
            logger.info(
                "Batch [%d:%d]: %d rows inserted.",
                batch_start,
                batch_start + len(batch),
                batch_inserted,
            )

        # LOGIC — single commit after all batches succeed
        connection.commit()
        logger.info(
            "Load complete. Total rows inserted: %d / %d.", rows_inserted, total_rows
        )
        return rows_inserted

    except Exception:
        # LOGIC — rollback the entire transaction on any failure
        connection.rollback()
        logger.exception(
            "Exception during position load; transaction rolled back."
        )
        raise

    finally:
        # BOILERPLATE — always close the connection
        connection.close()
        logger.info("Database connection closed.")