# BOILERPLATE
import logging
import math
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

from src.ingestion import secrets
from src.ingestion.exceptions import LoadError

logger = logging.getLogger(__name__)

# LOGIC
_BATCH_SIZE = 1000
_ET_TZ = pytz.timezone("America/Toronto")
_INSERT_SQL = """
    INSERT INTO rfdh.trade_positions
        (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
    VALUES %s
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df) -> int:
    # LOGIC — retrieve credentials and open DB connection
    creds = secrets.get_db_credentials()
    conn = None
    try:
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        total_inserted = _insert_batches(conn, valid_df)
        conn.commit()
        logger.info("Committed %d rows to rfdh.trade_positions", total_inserted)
        return total_inserted
    except Exception as exc:
        if conn is not None:
            conn.rollback()
            logger.error("Rolled back transaction due to error: %s", exc)
        raise LoadError(f"Failed to load positions into database: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()


def _insert_batches(conn, valid_df) -> int:
    # LOGIC — split DataFrame into batches and execute INSERT for each
    total_inserted = 0
    num_rows = len(valid_df)
    if num_rows == 0:
        logger.info("No valid rows to insert.")
        return 0

    num_batches = math.ceil(num_rows / _BATCH_SIZE)
    logger.info("Inserting %d rows in %d batch(es)", num_rows, num_batches)

    for batch_index in range(num_batches):
        start = batch_index * _BATCH_SIZE
        end = start + _BATCH_SIZE
        batch_df = valid_df.iloc[start:end]

        # LOGIC — one loaded_at timestamp per batch, ET-aware
        loaded_at = datetime.now(_ET_TZ)

        # LOGIC — build list of tuples matching INSERT column order
        rows = [
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                str(row["trade_date"]),
                str(row["instrument_type"]),
                float(row["notional_amount"]),
                str(row["currency"]),
                str(row["counterparty_id"]),
                loaded_at,
            )
            for _, row in batch_df.iterrows()
        ]

        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, _INSERT_SQL, rows)
            batch_inserted = cursor.rowcount
            # LOGIC — rowcount may be -1 for execute_values in some psycopg2 versions;
            # treat -1 as the batch size (all inserted) when conflict detection is uncertain
            if batch_inserted < 0:
                batch_inserted = len(rows)
            total_inserted += batch_inserted
            logger.info(
                "Batch %d/%d: attempted %d rows, inserted %d rows",
                batch_index + 1,
                num_batches,
                len(rows),
                batch_inserted,
            )

    return total_inserted