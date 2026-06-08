# BOILERPLATE
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import pytz

from pipeline_exceptions import DatabaseError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — target schema and table from infrastructure config
_SCHEMA_TABLE = "demo_schema.trade_positions"

# LOGIC — INSERT with ON CONFLICT deduplication on the composite PK
_INSERT_SQL = """
    INSERT INTO demo_schema.trade_positions
        (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _cast_row(row: pd.Series, loaded_at: datetime) -> tuple:
    # LOGIC — cast each column to its target database type before insert
    trade_id = str(row["trade_id"]).strip()
    desk_code = str(row["desk_code"]).strip()

    # LOGIC — parse trade_date string to datetime.date
    trade_date = datetime.strptime(str(row["trade_date"]).strip(), "%Y-%m-%d").date()

    instrument_type = str(row["instrument_type"]).strip()

    # LOGIC — cast notional_amount to Decimal for NUMERIC(20,4) precision
    notional_amount = Decimal(str(row["notional_amount"]).strip())

    # LOGIC — uppercase currency to enforce CHAR(3) canonical form
    currency = str(row["currency"]).strip().upper()

    counterparty_id = str(row["counterparty_id"]).strip()

    return (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)


def load_positions(valid_df: pd.DataFrame, conn) -> int:
    # LOGIC — load validated trade position rows; return count of rows actually inserted
    if valid_df.empty:
        logger.info("valid_df is empty; no rows to insert into %s", _SCHEMA_TABLE)
        return 0

    # LOGIC — capture ET timestamp once for the entire batch so all rows share the same loaded_at
    et_tz = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_tz)
    logger.info("Loading %d rows into %s with loaded_at=%s", len(valid_df), _SCHEMA_TABLE, loaded_at.isoformat())

    # LOGIC — build list of parameter tuples, casting all columns to target types
    rows = []
    for idx, row in valid_df.iterrows():
        try:
            rows.append(_cast_row(row, loaded_at))
        except (ValueError, InvalidOperation, KeyError) as exc:
            # LOGIC — individual cast failures should not have reached this point after validation;
            # log and skip rather than aborting the entire batch
            logger.error("Row cast failed at index %s: %s — skipping row", idx, exc)

    if not rows:
        logger.warning("All rows failed type casting; nothing to insert.")
        return 0

    try:
        cursor = conn.cursor()
        # LOGIC — executemany issues one INSERT per tuple; psycopg2 accumulates rowcount
        # for ON CONFLICT DO NOTHING: rowcount reflects only actually-inserted rows
        total_inserted = 0
        cursor.executemany(_INSERT_SQL, rows)
        # LOGIC — psycopg2 executemany sets cursor.rowcount to the total rows affected
        # (inserted); rows skipped by DO NOTHING are NOT counted
        total_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0
        logger.info(
            "executemany complete: %d rows submitted, %d rows inserted (deduplication may have skipped some)",
            len(rows),
            total_inserted,
        )
        return total_inserted
    except Exception as exc:
        logger.error("Database insert failed for %s: %s", _SCHEMA_TABLE, exc)
        raise DatabaseError(f"Failed to insert rows into {_SCHEMA_TABLE}: {exc}") from exc