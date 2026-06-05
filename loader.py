# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")

# BOILERPLATE — DBCredentials type expected from secrets.py
# Fields: host, port, dbname, username, password


def load_trades(valid_df, credentials, source_file: str) -> int:
    # LOGIC — bulk-insert validated trade rows into app.daily_trades
    # Returns the count of rows actually inserted (conflicts excluded)

    if valid_df.empty:
        logger.info("load_trades: valid_df is empty, nothing to insert.")
        return 0

    # LOGIC — capture load timestamp once in ET for the entire batch
    loaded_at = datetime.now(ET)

    # LOGIC — build list of tuples matching the INSERT column order
    rows = [
        (
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],          # datetime.date after validator cast
            str(row["instrument_type"]),
            float(row["notional_amount"]),
            str(row["currency"]),
            str(row["counterparty_id"]),
            loaded_at,
            source_file,
        )
        for _, row in valid_df.iterrows()
    ]

    insert_sql = """
        INSERT INTO app.daily_trades
            (trade_id, desk_code, trade_date, instrument_type, notional_amount,
             currency, counterparty_id, loaded_at, source_file)
        VALUES %s
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
    """

    conn = None
    cursor = None
    try:
        # BOILERPLATE — connect to Aurora with SSL required
        conn = psycopg2.connect(
            host=credentials.host,
            port=int(credentials.port),
            dbname=credentials.dbname,
            user=credentials.username,
            password=credentials.password,
            sslmode="require",
        )
        cursor = conn.cursor()

        # LOGIC — batch insert with page_size=1000 for performance (TAC-6)
        psycopg2.extras.execute_values(
            cursor,
            insert_sql,
            rows,
            template=None,
            page_size=1000,
        )

        # LOGIC — rowcount reflects only the rows not excluded by ON CONFLICT DO NOTHING
        rows_inserted = cursor.rowcount
        conn.commit()

        logger.info(
            "load_trades: inserted %d rows from source_file=%s (batch size=%d).",
            rows_inserted,
            source_file,
            len(rows),
        )
        return rows_inserted

    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                logger.error("load_trades: rollback failed: %s", rollback_exc)
        logger.exception(
            "load_trades: exception during insert for source_file=%s", source_file
        )
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()