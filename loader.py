# BOILERPLATE
import logging

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# LOGIC — SQL constants matching the data contract exactly
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

_PRE_COUNT_SQL = """
SELECT COUNT(*)
FROM demo_schema.trade_positions
WHERE desk_code = %s
  AND trade_date = %s
ORDER BY 1
"""

_POST_COUNT_SQL = """
SELECT COUNT(*)
FROM demo_schema.trade_positions
WHERE desk_code = %s
  AND trade_date = %s
ORDER BY 1
"""


def load_positions(
    valid_df: pd.DataFrame,
    db_credentials: dict,
) -> int:
    """
    Opens a psycopg2 connection to Aurora using db_credentials.
    Executes batched INSERT INTO demo_schema.trade_positions
      (trade_id, desk_code, trade_date, instrument_type,
       notional_amount, currency, counterparty_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING;
    Uses execute_values for batch performance.
    Commits transaction on success; rolls back on any exception.
    Returns count of rows actually inserted (not skipped).
    Raises RuntimeError wrapping original DB exception on failure.
    """
    # LOGIC — handle empty DataFrame: nothing to insert
    if valid_df.empty:
        logger.info("valid_df is empty; no rows to load.")
        return 0

    # LOGIC — extract desk_code and trade_date for scoped COUNT queries
    # The design specifies these as uniform values within a single file batch
    desk_code = str(valid_df["desk_code"].iloc[0])
    trade_date = str(valid_df["trade_date"].iloc[0])

    # LOGIC — build the list of row tuples in the exact column order matching the INSERT
    rows = [
        (
            str(row["trade_id"]),
            str(row["desk_code"]),
            str(row["trade_date"]),
            str(row["instrument_type"]),
            float(row["notional_amount"]),
            str(row["currency"]),
            str(row["counterparty_id"]),
        )
        for _, row in valid_df.iterrows()
    ]

    # BOILERPLATE — build psycopg2 connection from runtime credentials
    conn = None
    try:
        conn = psycopg2.connect(
            host=db_credentials["host"],
            port=int(db_credentials["port"]),
            dbname=db_credentials["dbname"],
            user=db_credentials["username"],
            password=db_credentials["password"],
            connect_timeout=10,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            # LOGIC — pre-insert count scoped to desk_code + trade_date
            cur.execute(_PRE_COUNT_SQL, (desk_code, trade_date))
            pre_count = cur.fetchone()[0]
            logger.info(
                "Pre-insert count for desk_code=%s trade_date=%s: %d",
                desk_code,
                trade_date,
                pre_count,
            )

            # LOGIC — batch insert using execute_values for performance
            psycopg2.extras.execute_values(
                cur,
                _INSERT_SQL,
                rows,
                template=None,
                page_size=500,
            )

            # LOGIC — post-insert count to compute net-new rows actually inserted
            cur.execute(_POST_COUNT_SQL, (desk_code, trade_date))
            post_count = cur.fetchone()[0]
            logger.info(
                "Post-insert count for desk_code=%s trade_date=%s: %d",
                desk_code,
                trade_date,
                post_count,
            )

        # LOGIC — commit only after both counts and insert succeed
        conn.commit()

        rows_inserted = post_count - pre_count
        logger.info(
            "Committed %d net-new rows for desk_code=%s trade_date=%s "
            "(%d submitted, %d skipped as duplicates).",
            rows_inserted,
            desk_code,
            trade_date,
            len(rows),
            len(rows) - rows_inserted,
        )
        return rows_inserted

    except Exception as exc:
        # LOGIC — roll back on any failure to leave DB in clean state
        if conn is not None:
            try:
                conn.rollback()
                logger.warning(
                    "Transaction rolled back for desk_code=%s trade_date=%s due to: %s",
                    desk_code,
                    trade_date,
                    exc,
                )
            except Exception as rollback_exc:
                logger.error(
                    "Rollback itself failed for desk_code=%s trade_date=%s: %s",
                    desk_code,
                    trade_date,
                    rollback_exc,
                )
        raise RuntimeError(
            f"Database load failed for desk_code={desk_code} trade_date={trade_date}: {exc}"
        ) from exc

    finally:
        # BOILERPLATE — always close connection to avoid connection pool exhaustion
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                logger.warning("Failed to close DB connection: %s", close_exc)