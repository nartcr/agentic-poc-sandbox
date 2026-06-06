# BOILERPLATE
import logging
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET = pytz.timezone("America/Toronto")

_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id, loaded_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df: pd.DataFrame, credentials: dict) -> int:
    # LOGIC — build row tuples and bulk-insert with conflict handling
    if valid_df.empty:
        logger.info("load_positions called with empty DataFrame; skipping insert.")
        return 0

    loaded_at = datetime.now(tz=_ET)

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

    conn = None
    try:
        # BOILERPLATE — open connection using credentials from Secrets Manager
        conn = psycopg2.connect(
            host=credentials["host"],
            port=int(credentials["port"]),
            user=credentials["username"],
            password=credentials["password"],
            dbname=credentials["dbname"],
        )
        with conn:
            with conn.cursor() as cursor:
                # LOGIC — execute_values performs a single multi-row INSERT
                psycopg2.extras.execute_values(
                    cursor,
                    _INSERT_SQL,
                    rows,
                    template=None,
                    page_size=1000,
                )
                # LOGIC — rowcount reflects rows actually inserted (conflicts excluded)
                inserted_count = cursor.rowcount
                logger.info(
                    "load_positions: attempted=%d inserted=%d",
                    len(rows),
                    inserted_count,
                )
        return inserted_count

    except psycopg2.Error as exc:
        # LOGIC — sanitise: do not log credential values
        logger.error(
            "Database error during load_positions: %s",
            type(exc).__name__,
        )
        raise RuntimeError(
            f"Failed to insert rows into demo_schema.trade_positions: {type(exc).__name__}"
        ) from exc
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass