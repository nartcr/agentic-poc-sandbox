# BOILERPLATE
import logging
import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def load_records(valid_df: pd.DataFrame, source_file: str, secrets: dict) -> int:
    """
    Bulk-insert validated trade records into rfdh.daily_trades.
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Returns the number of rows actually inserted (duplicates are silently skipped).
    Rolls back and re-raises on any error.
    """
    # LOGIC
    if valid_df.empty:
        logger.info("load_records: valid_df is empty, nothing to insert.")
        return 0

    # LOGIC — build list of tuples with correct types for psycopg2
    records = _build_records(valid_df, source_file)

    conn = None
    try:
        # BOILERPLATE — connect to Aurora PostgreSQL
        conn = psycopg2.connect(
            host=secrets["host"],
            port=int(secrets["port"]),
            dbname=secrets["dbname"],
            user=secrets["username"],
            password=secrets["password"],
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            # LOGIC — bulk insert with conflict handling
            insert_sql = """
                INSERT INTO rfdh.daily_trades (
                    trade_id,
                    desk_code,
                    trade_date,
                    instrument_type,
                    notional_amount,
                    currency,
                    counterparty_id,
                    source_file
                )
                VALUES %s
                ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
            """

            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                records,
                template=None,
                page_size=1000,
            )

            # LOGIC — rowcount reflects only rows actually inserted (DO NOTHING rows excluded)
            rows_inserted = cur.rowcount
            logger.info(
                "load_records: submitted=%d, inserted=%d, duplicates_skipped=%d, source_file=%s",
                len(records),
                rows_inserted,
                len(records) - rows_inserted,
                source_file,
            )

        conn.commit()
        logger.info("load_records: transaction committed successfully.")
        return rows_inserted

    except Exception:
        logger.exception("load_records: error during bulk insert — rolling back transaction.")
        if conn is not None:
            try:
                conn.rollback()
                logger.info("load_records: rollback completed.")
            except Exception:
                logger.exception("load_records: rollback also failed.")
        raise

    finally:
        # BOILERPLATE — always close connection
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.exception("load_records: failed to close database connection.")


def _build_records(valid_df: pd.DataFrame, source_file: str) -> list:
    """
    Convert each row of valid_df into a tuple matching the INSERT column order.
    Casts notional_amount to Decimal and trade_date to datetime.date.
    Raises ValueError if any cast fails (should not happen post-validation, but defensive).
    """
    # BOILERPLATE
    records = []

    for idx, row in valid_df.iterrows():
        # LOGIC — cast notional_amount string to Decimal
        try:
            notional = Decimal(str(row["notional_amount"]).strip())
        except InvalidOperation as exc:
            raise ValueError(
                f"Row index {idx}: cannot cast notional_amount "
                f"'{row['notional_amount']}' to Decimal."
            ) from exc

        # LOGIC — cast trade_date string to datetime.date
        try:
            trade_date = datetime.date.fromisoformat(str(row["trade_date"]).strip())
        except ValueError as exc:
            raise ValueError(
                f"Row index {idx}: cannot cast trade_date "
                f"'{row['trade_date']}' to date."
            ) from exc

        records.append((
            str(row["trade_id"]).strip(),
            str(row["desk_code"]).strip(),
            trade_date,
            str(row["instrument_type"]).strip(),
            notional,
            str(row["currency"]).strip(),
            str(row["counterparty_id"]).strip(),
            source_file,
        ))

    return records