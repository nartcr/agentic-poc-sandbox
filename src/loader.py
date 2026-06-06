# BOILERPLATE
import datetime
import logging

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC
ET = pytz.timezone("America/Toronto")


def load_trades(
    valid_df: pd.DataFrame,
    credentials: dict,
    source_file: str,
    loaded_at: datetime.datetime,
) -> int:
    """
    Loads validated trade rows into app.daily_trades using an upsert pattern.
    Rows whose (trade_id, desk_code, trade_date) already exist are silently skipped.
    Returns the count of rows actually inserted.
    """
    # LOGIC — enforce ET-awareness on loaded_at before any DB interaction
    assert (
        loaded_at.tzinfo is not None
        and loaded_at.tzinfo.utcoffset(loaded_at) is not None
    ), "loaded_at must be a timezone-aware datetime in ET"

    et_offset = pytz.timezone("America/Toronto").utcoffset(
        loaded_at.replace(tzinfo=None)
    )
    provided_offset = loaded_at.utcoffset()
    assert provided_offset == et_offset, (
        f"loaded_at timezone offset {provided_offset} does not match "
        f"America/Toronto offset {et_offset}"
    )

    if valid_df.empty:
        logger.info("valid_df is empty; skipping load_trades DB insert")
        return 0

    # LOGIC — build row tuples in the exact column order matching the INSERT statement
    rows = [
        (
            row["trade_id"],
            row["desk_code"],
            row["trade_date"],
            row["instrument_type"],
            float(row["notional_amount"]),
            row["currency"],
            row["counterparty_id"],
            loaded_at,
            source_file,
        )
        for _, row in valid_df.iterrows()
    ]

    # BOILERPLATE — build psycopg2 connection from credentials dict
    conn = psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )

    try:
        with conn:
            with conn.cursor() as cursor:
                # LOGIC — single-round-trip batched insert; ON CONFLICT DO NOTHING for dedup
                insert_sql = """
                    INSERT INTO app.daily_trades
                        (trade_id, desk_code, trade_date, instrument_type,
                         notional_amount, currency, counterparty_id,
                         loaded_at, source_file)
                    VALUES %s
                    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
                """
                psycopg2.extras.execute_values(cursor, insert_sql, rows)

                # LOGIC — rowcount reflects only rows actually inserted (not skipped by conflict)
                rows_inserted = cursor.rowcount
                logger.info(
                    "load_trades: attempted=%d inserted=%d skipped_duplicate=%d source_file=%s",
                    len(rows),
                    rows_inserted,
                    len(rows) - rows_inserted,
                    source_file,
                )

        return rows_inserted

    except Exception:
        logger.exception(
            "load_trades: exception during insert; rolling back. source_file=%s",
            source_file,
        )
        conn.rollback()
        raise
    finally:
        conn.close()