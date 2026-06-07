# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC — target table and columns defined by the data contract
_TARGET_TABLE = "demo_schema.trade_positions"
_INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "loaded_at",
    "source_file_key",
]

# LOGIC — SQL with ON CONFLICT DO NOTHING for idempotency (TAC-3)
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions (
    trade_id,
    desk_code,
    trade_date,
    instrument_type,
    notional_amount,
    currency,
    counterparty_id,
    loaded_at,
    source_file_key
)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _build_row_tuples(
    valid_df: pd.DataFrame,
    loaded_at: datetime,
    source_file_key: str,
) -> list[tuple]:
    # LOGIC — convert each DataFrame row into a tuple matching _INSERT_COLUMNS order
    rows = []
    for _, row in valid_df.iterrows():
        rows.append((
            str(row["trade_id"]).strip(),
            str(row["desk_code"]).strip(),
            str(row["trade_date"]).strip(),
            str(row["instrument_type"]).strip(),
            float(row["notional_amount"]),
            str(row["currency"]).strip(),
            str(row["counterparty_id"]).strip(),
            loaded_at,
            source_file_key,
        ))
    return rows


def load_positions(
    valid_df: pd.DataFrame,
    db_conn: psycopg2.extensions.connection,
    source_file_key: str,
) -> int:
    # LOGIC — bulk-insert valid positions; return actual rows inserted
    if valid_df.empty:
        logger.info("load_positions: valid_df is empty — nothing to insert")
        return 0

    # LOGIC — Eastern Time timestamp for loaded_at (TAC-7)
    et_tz = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_tz)

    logger.info(
        "load_positions: attempting to insert %d rows into %s",
        len(valid_df),
        _TARGET_TABLE,
    )

    row_tuples = _build_row_tuples(valid_df, loaded_at, source_file_key)

    try:
        with db_conn.cursor() as cursor:
            # LOGIC — execute_values for batch insert performance (TAC-1 / TAC-6)
            psycopg2.extras.execute_values(
                cursor,
                _INSERT_SQL,
                row_tuples,
                template=None,
                page_size=1000,
            )
            rows_inserted = cursor.rowcount

        db_conn.commit()

    except Exception as exc:
        logger.error(
            "load_positions: INSERT failed — rolling back. Error: %s",
            str(exc),
        )
        db_conn.rollback()
        raise

    # LOGIC — rowcount may be -1 for some drivers when using execute_values;
    # fall back to 0 in that case so callers always receive a non-negative int
    if rows_inserted < 0:
        logger.warning(
            "load_positions: cursor.rowcount returned %d after execute_values; "
            "defaulting rows_inserted to 0",
            rows_inserted,
        )
        rows_inserted = 0

    logger.info(
        "load_positions: committed — rows_inserted=%d (attempted=%d)",
        rows_inserted,
        len(valid_df),
    )

    return rows_inserted