# BOILERPLATE
import logging

import psycopg2
import psycopg2.extras

import secret_manager

logger = logging.getLogger(__name__)

# LOGIC — SQL for idempotent upsert; ON CONFLICT DO NOTHING satisfies TAC-3
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


# LOGIC
def _build_row_tuples(valid_df) -> list:
    """
    Converts valid_df rows into a list of tuples in INSERT column order.
    Column order must match the INSERT statement exactly.
    """
    rows = []
    for _, row in valid_df.iterrows():
        rows.append((
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],          # datetime.date — psycopg2 maps to DATE
            str(row["instrument_type"]),
            row["notional_amount"],     # Decimal — psycopg2 maps to NUMERIC
            str(row["currency"]),
            str(row["counterparty_id"]),
        ))
    return rows


# LOGIC
def load_positions(valid_df) -> int:
    """
    Batch-inserts validated trade position rows into demo_schema.trade_positions.

    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING for idempotency.
    Returns the count of rows actually inserted (conflicts are excluded).

    Args:
        valid_df: DataFrame with columns trade_id, desk_code, trade_date,
                  instrument_type, notional_amount, currency, counterparty_id.
                  trade_date must be datetime.date; notional_amount must be Decimal.

    Returns:
        int: number of rows inserted (0 if valid_df is empty or all conflicted).

    Raises:
        Exception: re-raises any database exception after rolling back.
    """
    # LOGIC — short-circuit if there is nothing to insert
    if valid_df is None or valid_df.empty:
        logger.info("load_positions called with empty DataFrame; 0 rows inserted.")
        return 0

    # BOILERPLATE — retrieve credentials from Secrets Manager (no hardcoded creds)
    creds = secret_manager.get_db_credentials()

    conn = None
    rows_inserted = 0
    rows_submitted = len(valid_df)

    try:
        # BOILERPLATE — open connection using runtime credentials
        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["dbname"],
            user=creds["username"],
            password=creds["password"],
        )
        cursor = conn.cursor()

        # LOGIC — build row tuples in the correct column order
        row_tuples = _build_row_tuples(valid_df)

        logger.info("Submitting %d rows to demo_schema.trade_positions.", rows_submitted)

        # LOGIC — execute batch insert; execute_values sends all rows in one round-trip
        psycopg2.extras.execute_values(
            cursor,
            _INSERT_SQL,
            row_tuples,
            template=None,
            page_size=1000,
        )

        # LOGIC — rowcount reflects actual rows inserted (DO NOTHING rows are excluded)
        rows_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0

        conn.commit()
        logger.info(
            "Committed: %d submitted, %d actually inserted (%d conflicts skipped).",
            rows_submitted,
            rows_inserted,
            rows_submitted - rows_inserted,
        )

    except Exception as exc:
        # LOGIC — rollback on any failure; re-raise so caller can handle audit/SNS
        if conn is not None:
            try:
                conn.rollback()
                logger.warning("Transaction rolled back due to exception: %s", exc)
            except Exception as rollback_exc:
                logger.error("Rollback itself failed: %s", rollback_exc)
        raise

    finally:
        # BOILERPLATE — always close the connection
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                logger.error("Error closing DB connection: %s", close_exc)

    return rows_inserted