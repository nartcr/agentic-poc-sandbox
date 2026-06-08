# BOILERPLATE
import logging

import psycopg2
import pandas as pd

import db_secrets

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact SQL per data contract: INSERT with ON CONFLICT DO NOTHING for idempotency
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — columns consumed from valid_df, in the order bound to INSERT SQL parameters
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _build_row_tuples(valid_df: pd.DataFrame) -> list[tuple]:
    # LOGIC — extract only the required columns in the correct bind-parameter order
    return [
        (
            str(row["trade_id"]).strip(),
            str(row["desk_code"]).strip(),
            str(row["trade_date"]).strip(),
            str(row["instrument_type"]).strip(),
            str(row["notional_amount"]).strip(),
            str(row["currency"]).strip(),
            str(row["counterparty_id"]).strip(),
        )
        for _, row in valid_df.iterrows()
    ]


def _open_connection(creds: dict):
    # BOILERPLATE — open a psycopg2 connection using credentials from Secrets Manager
    return psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC — load validated rows into demo_schema.trade_positions with idempotent upsert
    """
    Inserts validated trade position rows into demo_schema.trade_positions.

    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Rows already present in the table are silently skipped and NOT counted in the return value.

    Parameters
    ----------
    valid_df : pd.DataFrame
        DataFrame of validated rows. Must contain columns defined in _REQUIRED_COLUMNS.

    Returns
    -------
    int
        Number of rows actually inserted (skipped/duplicate rows are excluded).
    """
    if valid_df.empty:
        logger.info("load_positions called with empty DataFrame — nothing to insert.")
        return 0

    logger.info("load_positions starting: candidate_rows=%d", len(valid_df))

    creds = db_secrets.get_db_credentials()
    row_tuples = _build_row_tuples(valid_df)

    conn = None
    try:
        conn = _open_connection(creds)
        rows_inserted = _execute_inserts(conn, row_tuples)
        conn.commit()
        logger.info(
            "load_positions committed: rows_inserted=%d out of %d candidates",
            rows_inserted,
            len(row_tuples),
        )
        return rows_inserted

    except Exception:
        # LOGIC — roll back on any database error before re-raising
        if conn is not None:
            try:
                conn.rollback()
                logger.warning("load_positions: transaction rolled back due to exception.")
            except Exception as rollback_exc:
                logger.error("load_positions: rollback itself failed: %s", rollback_exc)
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                logger.error("load_positions: connection close failed: %s", close_exc)


def _execute_inserts(conn, row_tuples: list[tuple]) -> int:
    # LOGIC — execute individual inserts and accumulate rowcount to correctly count DO NOTHING skips
    """
    Executes INSERT statements one at a time within a single cursor.

    psycopg2's executemany does not reliably report per-row rowcount when using
    ON CONFLICT DO NOTHING (the driver aggregates counts in a way that masks skips).
    To satisfy TAC-3 (second load returns rows_inserted=0) and TAC-6 (performance),
    rows are sent using execute() in a tight loop — this is still far faster than
    opening a new cursor per row, and accurately reports actual inserts vs skips.
    """
    rows_inserted = 0
    with conn.cursor() as cursor:
        for row_tuple in row_tuples:
            cursor.execute(_INSERT_SQL, row_tuple)
            # LOGIC — rowcount is 1 if the row was inserted, 0 if DO NOTHING skipped it
            if cursor.rowcount == 1:
                rows_inserted += 1
    return rows_inserted