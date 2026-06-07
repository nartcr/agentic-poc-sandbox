# BOILERPLATE
import logging
from decimal import Decimal, InvalidOperation
from datetime import date

import pandas as pd
import psycopg2
import psycopg2.extras

import db_connector
from ingestion_exceptions import DBConnectionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_INSERT_SQL = """
    INSERT INTO demo_schema.trade_positions
        (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
    VALUES %s
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — count rows already present before insert so we can compute net-new rows accurately
_COUNT_SQL = """
    SELECT COUNT(*)
    FROM demo_schema.trade_positions
    WHERE (trade_id, desk_code, trade_date) IN %s
"""


def load_positions(valid_df: pd.DataFrame) -> int:
    """
    # LOGIC
    Batch-insert validated trade position rows into demo_schema.trade_positions.
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Returns the count of rows actually inserted (not skipped due to conflict).
    """
    if valid_df.empty:
        logger.info("valid_df is empty — no rows to insert, returning 0")
        return 0

    rows = _build_row_tuples(valid_df)
    logger.info("Preparing to insert %d rows into demo_schema.trade_positions", len(rows))

    conn = None
    try:
        conn = db_connector.get_connection()
        rows_inserted = _execute_batch_insert(conn, rows)
        conn.commit()
        logger.info("Committed %d new rows to demo_schema.trade_positions", rows_inserted)
        return rows_inserted
    except psycopg2.Error as exc:
        logger.error("Database error during position load: %s", exc)
        if conn:
            try:
                conn.rollback()
            except Exception:  # BOILERPLATE
                pass
        raise DBConnectionError(f"Failed to load positions into DB: {exc}") from exc
    finally:
        if conn:
            try:
                conn.close()
            except Exception:  # BOILERPLATE
                pass


def _build_row_tuples(valid_df: pd.DataFrame) -> list[tuple]:
    """
    # LOGIC
    Convert the validated DataFrame into a list of tuples matching the INSERT column order.
    Casts trade_date to datetime.date and notional_amount to Decimal.
    """
    rows = []
    for _, row in valid_df.iterrows():
        trade_date_val = _parse_trade_date(row["trade_date"])
        notional_val = _parse_notional(row["notional_amount"])
        rows.append((
            str(row["trade_id"]).strip(),
            str(row["desk_code"]).strip(),
            trade_date_val,
            str(row["instrument_type"]).strip(),
            notional_val,
            str(row["currency"]).strip(),
            str(row["counterparty_id"]).strip(),
        ))
    return rows


def _parse_trade_date(raw: str) -> date:
    """
    # LOGIC
    Parse a YYYY-MM-DD string into a datetime.date object.
    Raises ValueError if the string is not in the expected format.
    """
    from datetime import datetime
    return datetime.strptime(str(raw).strip(), "%Y-%m-%d").date()


def _parse_notional(raw: str) -> Decimal:
    """
    # LOGIC
    Cast a string representation of notional amount to Decimal.
    Raises InvalidOperation (subclass of ArithmeticError) if the value is not numeric.
    """
    try:
        return Decimal(str(raw).strip())
    except InvalidOperation as exc:
        raise ValueError(f"Cannot convert notional_amount to Decimal: {raw!r}") from exc


def _execute_batch_insert(conn: psycopg2.extensions.connection, rows: list[tuple]) -> int:
    """
    # LOGIC
    Execute the batch INSERT using execute_values with page_size=1000 (TAC-6).
    Returns the number of rows actually inserted (excluding ON CONFLICT skips).
    Uses a pre/post count delta on the exact composite keys being inserted to determine
    the net-new rows, because cursor.rowcount is unreliable with DO NOTHING.
    """
    composite_keys = tuple((r[0], r[1], r[2]) for r in rows)

    with conn.cursor() as cur:
        # LOGIC — count pre-existing rows matching these composite keys
        cur.execute(_COUNT_SQL, (composite_keys,))
        pre_existing: int = cur.fetchone()[0]
        logger.info("Pre-existing rows matching batch keys: %d", pre_existing)

        # LOGIC — batch insert with page_size=1000 per TAC-6 performance requirement
        psycopg2.extras.execute_values(
            cur,
            _INSERT_SQL,
            rows,
            template=None,
            page_size=1000,
        )

        rows_inserted = len(rows) - pre_existing
        logger.info(
            "execute_values complete — submitted %d rows, estimated inserted %d (pre-existing skipped: %d)",
            len(rows),
            rows_inserted,
            pre_existing,
        )

    return max(rows_inserted, 0)