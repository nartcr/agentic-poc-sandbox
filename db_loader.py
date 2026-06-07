# BOILERPLATE
import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Optional

import psycopg2
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE

# LOGIC — SQL for idempotent bulk insert per TAC-3
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions (
    trade_id,
    desk_code,
    trade_date,
    instrument_type,
    notional_amount,
    currency,
    counterparty_id,
    source_file_key,
    loaded_at_et
)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — placeholder template for psycopg2 execute_values
_INSERT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s)"


def _to_decimal(value: str) -> Decimal:
    # LOGIC — convert notional_amount string to Decimal for NUMERIC(20,4) precision
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Cannot convert notional_amount to Decimal: {value!r}") from exc


def _build_row_tuple(
    row: pd.Series,
    loaded_at_et: datetime,
    source_file_key: str,
) -> tuple:
    # LOGIC — map DataFrame row to INSERT parameter tuple matching column order
    return (
        str(row["trade_id"]),
        str(row["desk_code"]),
        str(row["trade_date"]),        # psycopg2 accepts YYYY-MM-DD string for DATE
        str(row["instrument_type"]),
        _to_decimal(row["notional_amount"]),
        str(row["currency"]),
        str(row["counterparty_id"]),
        source_file_key,
        loaded_at_et,
    )


def load_positions(
    valid_df: pd.DataFrame,
    credentials,                        # DBCredentials namedtuple from secret_manager
    batch_size: int = 1000,
    source_file_key: str = "",          # LOGIC — populated by lambda_handler; absent from approved sig, added as optional
) -> int:
    """
    Bulk-insert valid trade positions into demo_schema.trade_positions.
    Returns the count of rows actually inserted (excluding ON CONFLICT DO NOTHING skips).
    Idempotent: re-running with the same rows does not produce duplicates (TAC-3).
    """
    # LOGIC — nothing to do if valid_df is empty
    if valid_df.empty:
        logger.info("valid_df is empty — no rows to load.")
        return 0

    # LOGIC — capture ET timestamp once for all rows in this batch run (TAC-7)
    loaded_at_et: datetime = datetime.now(tz=ET)

    # BOILERPLATE — build connection from runtime credentials (no hardcoded secrets, BAC-8)
    conn = psycopg2.connect(
        host=credentials.host,
        port=int(credentials.port),
        user=credentials.username,
        password=credentials.password,
        dbname=credentials.dbname,
    )
    conn.autocommit = False  # LOGIC — explicit transaction management

    rows_inserted: int = 0

    try:
        with conn.cursor() as cursor:
            # LOGIC — convert DataFrame to list of tuples once, then slice into batches
            all_tuples = [
                _build_row_tuple(row, loaded_at_et, source_file_key)
                for _, row in valid_df.iterrows()
            ]

            total_rows = len(all_tuples)
            num_batches = (total_rows + batch_size - 1) // batch_size

            for batch_idx in range(num_batches):
                start = batch_idx * batch_size
                end = start + batch_size
                batch = all_tuples[start:end]

                # LOGIC — use execute_values for efficient bulk insert
                psycopg2.extras.execute_values(
                    cursor,
                    _INSERT_SQL,
                    batch,
                    template=_INSERT_TEMPLATE,
                    page_size=batch_size,
                )

                # LOGIC — cursor.rowcount reflects only rows inserted (DO NOTHING rows not counted)
                batch_inserted = cursor.rowcount
                rows_inserted += batch_inserted

                logger.debug(
                    "Batch %d/%d: submitted %d rows, inserted %d new rows.",
                    batch_idx + 1,
                    num_batches,
                    len(batch),
                    batch_inserted,
                )

        # LOGIC — commit only after all batches succeed
        conn.commit()
        logger.info(
            "load_positions complete: %d total rows submitted, %d inserted, %d skipped (duplicate).",
            total_rows,
            rows_inserted,
            total_rows - rows_inserted,
        )

    except Exception:
        # LOGIC — roll back the entire transaction on any failure
        conn.rollback()
        logger.exception("load_positions failed — transaction rolled back.")
        raise

    finally:
        # BOILERPLATE — always close connection
        conn.close()

    return rows_inserted