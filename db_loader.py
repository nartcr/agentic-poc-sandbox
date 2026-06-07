# BOILERPLATE
import logging
from typing import List, Tuple

import pandas as pd
import psycopg2
import psycopg2.extensions
import psycopg2.extras

import secrets_client

logger = logging.getLogger(__name__)

# LOGIC — column order must match the INSERT statement exactly
_INSERT_COLUMNS: List[str] = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

_COUNT_SQL = """
SELECT COUNT(*)
FROM demo_schema.trade_positions
WHERE (desk_code, trade_date) IN %s
"""


def load_positions(valid_df: pd.DataFrame) -> int:
    # LOGIC
    """
    Batch-insert validated position rows into demo_schema.trade_positions.

    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for
    idempotent re-runs (TAC-3). Returns the count of net-new rows inserted,
    determined by comparing pre- and post-insert counts for the
    (desk_code, trade_date) combinations present in the batch.

    Args:
        valid_df: DataFrame with columns matching _INSERT_COLUMNS.

    Returns:
        int: number of rows actually inserted (not skipped by conflict).
    """
    if valid_df.empty:
        logger.info("valid_df is empty — nothing to load.")
        return 0

    conn = _get_connection()
    try:
        # LOGIC — build the set of (desk_code, trade_date) pairs for count queries
        desk_date_pairs = list(
            {
                (row["desk_code"], str(row["trade_date"]))
                for _, row in valid_df.iterrows()
            }
        )
        desk_date_tuple = tuple(desk_date_pairs)

        insert_tuples = _build_insert_tuples(valid_df)

        with conn.cursor() as cur:
            # LOGIC — pre-insert count scoped to the exact (desk_code, trade_date) combos
            cur.execute(_COUNT_SQL, (desk_date_tuple,))
            pre_count: int = cur.fetchone()[0]
            logger.info(
                "Pre-insert count for batch key(s) %s: %d", desk_date_pairs, pre_count
            )

            # LOGIC — bulk insert with conflict handling
            psycopg2.extras.execute_values(
                cur,
                _INSERT_SQL,
                insert_tuples,
                template=None,
                page_size=1000,
            )

            # LOGIC — post-insert count to compute net-new rows
            cur.execute(_COUNT_SQL, (desk_date_tuple,))
            post_count: int = cur.fetchone()[0]
            logger.info(
                "Post-insert count for batch key(s) %s: %d", desk_date_pairs, post_count
            )

        conn.commit()

        rows_inserted = post_count - pre_count
        logger.info(
            "load_positions complete. rows_in_batch=%d rows_inserted=%d rows_skipped=%d",
            len(valid_df),
            rows_inserted,
            len(valid_df) - rows_inserted,
        )
        return rows_inserted

    except Exception:
        logger.exception("Error during load_positions — rolling back transaction.")
        conn.rollback()
        raise
    finally:
        conn.close()


def _get_connection() -> psycopg2.extensions.connection:
    # LOGIC
    """
    Open a new psycopg2 connection using credentials from Secrets Manager.

    Maps 'username' (Secrets Manager key) to 'user' (psycopg2 parameter).
    """
    creds = secrets_client.get_db_credentials()
    logger.debug(
        "Opening psycopg2 connection. host=%s dbname=%s", creds["host"], creds["dbname"]
    )
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["username"],   # LOGIC — Secrets Manager uses 'username'; psycopg2 uses 'user'
        password=creds["password"],
    )
    conn.autocommit = False
    return conn


def _build_insert_tuples(df: pd.DataFrame) -> List[Tuple]:
    # LOGIC
    """
    Convert the DataFrame into a list of tuples in INSERT column order.

    Column order: trade_id, desk_code, trade_date, instrument_type,
                  notional_amount, currency, counterparty_id

    trade_date is cast to Python date (psycopg2 maps date → PostgreSQL DATE).
    notional_amount is cast to float (psycopg2 maps float → PostgreSQL NUMERIC).
    """
    tuples: List[Tuple] = []
    for _, row in df.iterrows():
        tuples.append(
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                pd.Timestamp(row["trade_date"]).date(),   # LOGIC — ensure Python date type
                str(row["instrument_type"]),
                float(row["notional_amount"]),             # LOGIC — ensure float for NUMERIC
                str(row["currency"]),
                str(row["counterparty_id"]),
            )
        )
    return tuples