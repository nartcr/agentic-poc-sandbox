# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

import exceptions
import secrets as secrets_module

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")

# LOGIC
def load_positions(valid_df, source_file: str) -> int:
    """
    Inserts validated trade positions into rfdh.trade_positions.
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Returns the count of rows actually inserted (not skipped).
    """
    if valid_df is None or len(valid_df) == 0:
        logger.info("load_positions called with empty DataFrame; skipping insert.")
        return 0

    # LOGIC — retrieve credentials at runtime, never hardcoded
    try:
        creds = secrets_module.get_db_credentials()
    except Exception as exc:
        logger.error("Failed to retrieve DB credentials: %s", exc)
        raise exceptions.LoadError(f"Credential retrieval failed: {exc}") from exc

    # LOGIC — compute ET timestamp for loaded_at
    loaded_at = datetime.now(tz=ET)

    conn = None
    cursor = None
    try:
        # BOILERPLATE — establish connection
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        cursor = conn.cursor()

        # LOGIC — capture pre-insert count for the rows in valid_df
        # to compute net inserted count reliably with ON CONFLICT DO NOTHING
        trade_keys = [
            (str(row["trade_id"]), str(row["desk_code"]), row["trade_date"])
            for _, row in valid_df.iterrows()
        ]

        # LOGIC — count how many of the incoming (trade_id, desk_code, trade_date)
        # tuples already exist in the table
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM rfdh.trade_positions
            WHERE (trade_id, desk_code, trade_date) IN %s
            """,
            (tuple(trade_keys),),
        )
        pre_existing_count = cursor.fetchone()[0]

        # LOGIC — build the rows tuple for batch insert
        rows = [
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                row["trade_date"],             # datetime.date from validator
                str(row["instrument_type"]),
                float(row["notional_amount"]), # float64 from validator
                str(row["currency"]),
                str(row["counterparty_id"]),
                loaded_at,
                source_file,
            )
            for _, row in valid_df.iterrows()
        ]

        # LOGIC — idempotent batch insert
        insert_sql = """
            INSERT INTO rfdh.trade_positions
              (trade_id, desk_code, trade_date, instrument_type, notional_amount,
               currency, counterparty_id, loaded_at, source_file)
            VALUES %s
            ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
        """
        psycopg2.extras.execute_values(cursor, insert_sql, rows, page_size=500)
        conn.commit()

        # LOGIC — net inserted = total attempted minus pre-existing duplicates
        inserted_count = len(rows) - pre_existing_count
        logger.info(
            "load_positions: attempted=%d, pre_existing=%d, inserted=%d, source_file=%s",
            len(rows),
            pre_existing_count,
            inserted_count,
            source_file,
        )
        return inserted_count

    except exceptions.LoadError:
        raise
    except Exception as exc:
        logger.error("DB insert failed, rolling back: %s", exc)
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rb_exc:
                logger.error("Rollback also failed: %s", rb_exc)
        raise exceptions.LoadError(f"DB insert failed: {exc}") from exc
    finally:
        # BOILERPLATE — always close resources
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass