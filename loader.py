# BOILERPLATE
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import psycopg2
import psycopg2.extras

from secrets import DbCredentials  # noqa: F401 — type reference

logger = logging.getLogger(__name__)

# LOGIC — SQL template per approved design
_INSERT_SQL = """
INSERT INTO rfdh.trade_positions
  (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, processed_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — ordered list of columns extracted from valid_df for insert
_INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _build_connection(db_credentials: "DbCredentials") -> psycopg2.extensions.connection:
    # LOGIC — build psycopg2 connection from typed credentials dataclass
    logger.debug(
        "Opening DB connection to host=%s port=%s dbname=%s user=%s",
        db_credentials.host,
        db_credentials.port,
        db_credentials.dbname,
        db_credentials.username,
    )
    conn = psycopg2.connect(
        host=db_credentials.host,
        port=db_credentials.port,
        dbname=db_credentials.dbname,
        user=db_credentials.username,
        password=db_credentials.password,
    )
    return conn


def _parse_trade_date(trade_date_str: str) -> "datetime.date":
    # LOGIC — convert YYYYMMDD string to date object for PostgreSQL DATE column
    from datetime import date as _date
    return _date(
        int(trade_date_str[0:4]),
        int(trade_date_str[4:6]),
        int(trade_date_str[6:8]),
    )


def _row_to_tuple(row: pd.Series, processed_at: datetime) -> tuple:
    # LOGIC — convert a valid DataFrame row to a tuple matching the INSERT column order
    trade_date_val = _parse_trade_date(str(row["trade_date"]))

    try:
        notional = Decimal(str(row["notional_amount"]))
    except InvalidOperation:
        # This should not reach loader — validator already confirmed convertibility
        raise ValueError(
            f"notional_amount could not be converted to Decimal: {row['notional_amount']!r}"
        )

    return (
        str(row["trade_id"]),
        str(row["desk_code"]),
        trade_date_val,
        str(row["instrument_type"]),
        notional,
        str(row["currency"]),
        str(row["counterparty_id"]),
        processed_at,
    )


def load_positions(
    valid_df: pd.DataFrame,
    db_credentials: "DbCredentials",
    processed_at: datetime,
) -> int:
    # LOGIC — batch-upsert valid rows into rfdh.trade_positions; return actual insert count
    if valid_df.empty:
        logger.info("valid_df is empty — nothing to load")
        return 0

    # LOGIC — build list of tuples in insert-column order
    rows = [_row_to_tuple(row, processed_at) for _, row in valid_df.iterrows()]

    logger.info("Attempting to load %d rows into rfdh.trade_positions", len(rows))

    conn = _build_connection(db_credentials)
    try:
        with conn.cursor() as cur:
            # LOGIC — single batch insert; execute_values expands VALUES %s placeholder
            psycopg2.extras.execute_values(
                cur,
                _INSERT_SQL,
                rows,
                page_size=1000,
            )
            rows_inserted = cur.rowcount
            logger.info(
                "execute_values rowcount=%d (inserted); %d rows were conflict-skipped",
                rows_inserted,
                len(rows) - rows_inserted,
            )
        conn.commit()
        logger.info("DB commit successful")
    except Exception:
        logger.exception("DB insert failed — rolling back")
        conn.rollback()
        raise
    finally:
        conn.close()

    return rows_inserted