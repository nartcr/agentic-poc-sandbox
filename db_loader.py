# BOILERPLATE
import logging
import os
from decimal import Decimal, InvalidOperation

import pandas as pd
import psycopg2

from secret_helper import get_secret

logger = logging.getLogger(__name__)

# LOGIC — batch size per TAC-6: executemany in batches of 500 rows
_BATCH_SIZE = 500

# LOGIC — explicit INSERT with ON CONFLICT DO NOTHING for idempotency (TAC-3)
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
    VALUES (
        %(trade_id)s,
        %(desk_code)s,
        %(trade_date)s,
        %(instrument_type)s,
        %(notional_amount)s,
        %(currency)s,
        %(counterparty_id)s
    )
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def get_db_connection(secret_id: str) -> psycopg2.extensions.connection:
    # LOGIC — retrieve credentials from Secrets Manager; no literals in code (TAC-8)
    secret = get_secret(secret_id)

    host = secret["host"]
    port = int(secret["port"])
    dbname = secret["dbname"]
    username = secret["username"]
    password = secret["password"]

    logger.info(
        "Opening database connection to host=%s port=%d dbname=%s",
        host,
        port,
        dbname,
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password,
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn


def count_inserted(cursor: psycopg2.extensions.cursor, batch_rowcount: int) -> int:
    # LOGIC — psycopg2 cursor.rowcount after executemany reflects rows affected
    # For INSERT ... ON CONFLICT DO NOTHING, affected rows = rows actually inserted
    # This function normalises the rowcount value for a single batch:
    #   -1 means the driver could not determine count; treat as 0 to be conservative
    if batch_rowcount < 0:
        logger.warning(
            "cursor.rowcount returned %d after executemany batch; treating as 0",
            batch_rowcount,
        )
        return 0
    return batch_rowcount


def load_positions(
    conn: psycopg2.extensions.connection, valid_df: pd.DataFrame
) -> int:
    # LOGIC — convert DataFrame rows to list of dicts for executemany
    if valid_df.empty:
        logger.info("valid_df is empty; no rows to insert")
        return 0

    # LOGIC — build row dicts with exact column names matching the INSERT statement
    rows = []
    for _, row in valid_df.iterrows():
        rows.append(
            {
                "trade_id": str(row["trade_id"]),
                "desk_code": str(row["desk_code"]),
                "trade_date": row["trade_date"],  # psycopg2 handles date/str
                "instrument_type": str(row["instrument_type"]),
                "notional_amount": _coerce_notional(row["notional_amount"]),
                "currency": str(row["currency"]),
                "counterparty_id": str(row["counterparty_id"]),
            }
        )

    total_inserted = 0
    total_rows = len(rows)

    with conn.cursor() as cursor:
        # LOGIC — process in batches of _BATCH_SIZE (500) per TAC-6
        for batch_start in range(0, total_rows, _BATCH_SIZE):
            batch = rows[batch_start : batch_start + _BATCH_SIZE]
            cursor.executemany(_INSERT_SQL, batch)
            batch_inserted = count_inserted(cursor, cursor.rowcount)
            total_inserted += batch_inserted
            logger.info(
                "Batch rows %d–%d: inserted=%d",
                batch_start,
                batch_start + len(batch) - 1,
                batch_inserted,
            )

    conn.commit()
    logger.info(
        "load_positions complete: total_rows=%d rows_inserted=%d rows_skipped=%d",
        total_rows,
        total_inserted,
        total_rows - total_inserted,
    )
    return total_inserted


def _coerce_notional(value) -> Decimal:
    # LOGIC — cast notional_amount to Decimal for NUMERIC(20,4) column precision
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        # row_validator should have caught this; log and re-raise to surface data issue
        raise ValueError(
            "notional_amount cannot be cast to Decimal: %r" % value
        ) from exc