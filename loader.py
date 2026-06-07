# BOILERPLATE
import logging
from datetime import datetime, date

import psycopg2
import psycopg2.extras
import pytz
import pandas as pd

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

# LOGIC
def _parse_trade_date(value) -> date:
    """
    Convert a trade_date value (string YYYYMMDD or datetime.date) to a
    Python datetime.date so PostgreSQL's DATE column accepts it.
    """
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # Treat as YYYYMMDD string
    return datetime.strptime(str(value).strip(), "%Y%m%d").date()


# LOGIC
def _build_insert_tuples(valid_df: pd.DataFrame, loaded_at: datetime) -> list[tuple]:
    """
    Convert each valid row into a tuple matching the INSERT column list:
        (trade_id, desk_code, trade_date, instrument_type,
         notional_amount, currency, counterparty_id, loaded_at)
    """
    rows = []
    for _, row in valid_df.iterrows():
        rows.append((
            str(row["trade_id"]).strip(),
            str(row["desk_code"]).strip(),
            _parse_trade_date(row["trade_date"]),
            str(row["instrument_type"]).strip(),
            float(row["notional_amount"]),
            str(row["currency"]).strip(),
            str(row["counterparty_id"]).strip(),
            loaded_at,
        ))
    return rows


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

# LOGIC
def load_positions(valid_df: pd.DataFrame, conn) -> int:
    """
    Batch-insert validated rows into demo_schema.trade_positions using
    INSERT ... ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING.

    Returns the count of rows actually inserted (duplicates not counted).

    The caller is responsible for commit/rollback; this function does NOT
    commit or roll back the connection.
    """
    if valid_df.empty:
        logger.info("valid_df is empty; nothing to insert.")
        return 0

    et_tz = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_tz)

    insert_rows = _build_insert_tuples(valid_df, loaded_at)

    # LOGIC — extract the dedup keys so we can measure how many already exist
    dedup_keys = [
        (str(row["trade_id"]).strip(), str(row["desk_code"]).strip(), _parse_trade_date(row["trade_date"]))
        for _, row in valid_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO demo_schema.trade_positions
            (trade_id, desk_code, trade_date, instrument_type,
             notional_amount, currency, counterparty_id, loaded_at)
        VALUES %s
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
    """

    with conn.cursor() as cur:
        # LOGIC — count existing rows matching the dedup key set BEFORE insert
        psycopg2.extras.execute_values(
            cur,
            """
            SELECT COUNT(*) FROM demo_schema.trade_positions
            WHERE (trade_id, desk_code, trade_date) IN %s
            """,
            [tuple(dedup_keys)],
            template=None,
            page_size=len(dedup_keys),
        )
        row = cur.fetchone()
        existing_count = row[0] if row else 0

        # LOGIC — perform the batch insert
        psycopg2.extras.execute_values(
            cur,
            insert_sql,
            insert_rows,
            template=None,
            page_size=500,
        )

    # LOGIC — rows actually inserted = total attempted − rows that already existed
    rows_inserted = len(insert_rows) - existing_count

    logger.info(
        "load_positions: attempted=%d existing=%d inserted=%d",
        len(insert_rows),
        existing_count,
        rows_inserted,
    )
    return rows_inserted