# BOILERPLATE
import logging
import math
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)

# LOGIC — column order must match rfdh.trade_positions schema exactly
_POSITIONS_COLUMNS = (
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "loaded_at",
)

_INSERT_SQL = (
    "INSERT INTO rfdh.trade_positions "
    "(trade_id, desk_code, trade_date, instrument_type, "
    "notional_amount, currency, counterparty_id, loaded_at) "
    "VALUES %s "
    "ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING"
)

_CHUNK_SIZE = 1000


def load_positions(valid_df, db_credentials: dict) -> int:
    # LOGIC — establish single connection for the entire load call
    conn = psycopg2.connect(
        host=db_credentials["host"],
        port=int(db_credentials["port"]),
        dbname=db_credentials["dbname"],
        user=db_credentials["username"],
        password=db_credentials["password"],
    )

    # LOGIC — single ET timestamp applied to all rows in this load batch (TAC-7)
    et_tz = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_tz)

    logger.info(
        "Starting load_positions: %d valid rows, loaded_at=%s",
        len(valid_df),
        loaded_at.isoformat(),
    )

    rows_inserted = 0

    try:
        with conn:
            with conn.cursor() as cur:
                # LOGIC — build list of tuples matching column order
                records = [
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
                    for _, row in valid_df.iterrows()
                ]

                # LOGIC — batch insert in chunks of 1,000 (TAC-6 performance)
                total_records = len(records)
                num_chunks = math.ceil(total_records / _CHUNK_SIZE) if total_records > 0 else 0

                for chunk_idx in range(num_chunks):
                    chunk_start = chunk_idx * _CHUNK_SIZE
                    chunk_end = chunk_start + _CHUNK_SIZE
                    chunk = records[chunk_start:chunk_end]

                    psycopg2.extras.execute_values(
                        cur,
                        _INSERT_SQL,
                        chunk,
                        template=None,
                        page_size=_CHUNK_SIZE,
                    )

                    # LOGIC — rowcount reflects actual inserts after DO NOTHING suppression
                    chunk_inserted = cur.rowcount if cur.rowcount >= 0 else 0
                    rows_inserted += chunk_inserted

                    logger.info(
                        "Chunk %d/%d: %d rows submitted, %d rows inserted",
                        chunk_idx + 1,
                        num_chunks,
                        len(chunk),
                        chunk_inserted,
                    )

        logger.info(
            "load_positions complete: %d/%d rows inserted (ON CONFLICT suppressed %d)",
            rows_inserted,
            total_records,
            total_records - rows_inserted,
        )

    except Exception:
        # LOGIC — explicit rollback before re-raise so connection is left clean
        conn.rollback()
        logger.exception("load_positions failed; transaction rolled back")
        raise
    finally:
        conn.close()

    return rows_inserted