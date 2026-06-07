# BOILERPLATE
import logging
from datetime import datetime
from typing import List, Tuple

import pandas as pd
import psycopg2
import pytz

from exceptions import LoadError
from secrets import DBCredentials

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — batch size per executemany call (TAC-6: 10 batches for 10,000 rows)
_BATCH_SIZE = 1000

# LOGIC — SQL statement matching the data contract exactly
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
  (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _get_et_now() -> datetime:
    # LOGIC — all timestamps in ET (America/Toronto) per TAC-7
    return datetime.now(pytz.timezone("America/Toronto"))


def _build_batches(rows: List[Tuple], batch_size: int) -> List[List[Tuple]]:
    # LOGIC — split flat list of row tuples into fixed-size batches
    return [rows[i : i + batch_size] for i in range(0, len(rows), batch_size)]


def _rows_to_tuples(valid_df: pd.DataFrame, loaded_at: datetime) -> List[Tuple]:
    # LOGIC — convert DataFrame to list of tuples in column order matching INSERT_SQL
    tuples = []
    for _, row in valid_df.iterrows():
        tuples.append((
            str(row["trade_id"]),
            str(row["desk_code"]),
            str(row["trade_date"]),
            str(row["instrument_type"]),
            float(row["notional_amount"]),
            str(row["currency"]),
            str(row["counterparty_id"]),
            loaded_at,
        ))
    return tuples


def load_positions(valid_df: pd.DataFrame, credentials: DBCredentials) -> int:
    """
    Load validated trade positions into demo_schema.trade_positions using an
    idempotent upsert.  Returns the count of newly inserted rows.

    Satisfies: BAC-1 (valid rows loaded), BAC-3 (ON CONFLICT DO NOTHING),
               BAC-6 (batch loading), TAC-3 (idempotency), TAC-6 (performance).
    """
    # LOGIC — stamp loaded_at once for the entire file load
    loaded_at = _get_et_now()
    logger.info(
        "load_positions called: %d rows to load, loaded_at=%s",
        len(valid_df),
        loaded_at.isoformat(),
    )

    if valid_df.empty:
        logger.info("valid_df is empty — nothing to load, returning 0")
        return 0

    # LOGIC — build row tuples before opening the connection
    row_tuples = _rows_to_tuples(valid_df, loaded_at)
    batches = _build_batches(row_tuples, _BATCH_SIZE)
    logger.info(
        "Prepared %d row tuples across %d batch(es) of up to %d rows",
        len(row_tuples),
        len(batches),
        _BATCH_SIZE,
    )

    conn = None
    try:
        # BOILERPLATE — open a single psycopg2 connection reused across all batches
        conn = psycopg2.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.username,
            password=credentials.password,
            dbname=credentials.dbname,
        )
        conn.autocommit = False

        total_inserted = 0
        with conn.cursor() as cursor:
            for batch_index, batch in enumerate(batches):
                # LOGIC — executemany for the batch; rowcount reflects actual inserts
                cursor.executemany(_INSERT_SQL, batch)
                batch_inserted = cursor.rowcount
                # LOGIC — psycopg2 returns -1 for rowcount on executemany in some
                # driver versions; guard against that by treating -1 as the batch size
                # Note: with ON CONFLICT DO NOTHING, rowcount counts only inserted rows
                # in PostgreSQL 9.6+ when using libpq protocol level >= 3.
                if batch_inserted < 0:
                    logger.warning(
                        "cursor.rowcount returned %d for batch %d; "
                        "falling back to batch length %d",
                        batch_inserted,
                        batch_index,
                        len(batch),
                    )
                    batch_inserted = len(batch)
                total_inserted += batch_inserted
                logger.debug(
                    "Batch %d/%d: %d row(s) inserted",
                    batch_index + 1,
                    len(batches),
                    batch_inserted,
                )

        # LOGIC — single commit after all batches succeed (TAC-1, BAC-6)
        conn.commit()
        logger.info(
            "All batches committed successfully. Total rows inserted: %d",
            total_inserted,
        )
        return total_inserted

    except Exception as exc:
        # LOGIC — rollback on any failure to leave the connection clean
        if conn is not None:
            try:
                conn.rollback()
                logger.warning("Transaction rolled back due to exception: %s", exc)
            except Exception as rollback_exc:
                logger.error("Rollback itself failed: %s", rollback_exc)
        logger.error("load_positions failed: %s", exc, exc_info=True)
        raise LoadError(f"Failed to load positions into Aurora: {exc}") from exc

    finally:
        # BOILERPLATE — always close connection
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                logger.warning("Failed to close DB connection: %s", close_exc)