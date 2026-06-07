# BOILERPLATE
import logging
import os
from datetime import date
from decimal import Decimal, InvalidOperation

import pandas as pd
import psycopg2.extras

from db_connection import get_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — batch size for executemany-style inserts (TAC-6)
_BATCH_SIZE = 1000

# LOGIC — exact SQL as specified in the approved design; ON CONFLICT ensures idempotency (TAC-3)
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _coerce_row(row: pd.Series) -> tuple:
    """
    Coerce a single DataFrame row to the tuple of Python types expected
    by psycopg2 for the trade_positions INSERT.
    """
    # LOGIC — trade_date: str → datetime.date
    try:
        trade_date_val = date.fromisoformat(str(row["trade_date"]).strip())
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Cannot coerce trade_date '{row['trade_date']}' to date: {exc}"
        ) from exc

    # LOGIC — notional_amount: str → Decimal
    try:
        notional_val = Decimal(str(row["notional_amount"]).strip())
    except (InvalidOperation, TypeError) as exc:
        raise ValueError(
            f"Cannot coerce notional_amount '{row['notional_amount']}' to Decimal: {exc}"
        ) from exc

    return (
        str(row["trade_id"]).strip(),
        str(row["desk_code"]).strip(),
        trade_date_val,
        str(row["instrument_type"]).strip(),
        notional_val,
        str(row["currency"]).strip(),
        str(row["counterparty_id"]).strip(),
    )


def _build_batches(valid_df: pd.DataFrame) -> "list[list[tuple]]":
    """
    Convert valid_df into a list of batches, each batch being a list of
    coerced row tuples of at most _BATCH_SIZE rows.
    """
    # LOGIC — iterate rows and accumulate batches
    batches: list[list[tuple]] = []
    current_batch: list[tuple] = []

    for _, row in valid_df.iterrows():
        current_batch.append(_coerce_row(row))
        if len(current_batch) == _BATCH_SIZE:
            batches.append(current_batch)
            current_batch = []

    if current_batch:
        batches.append(current_batch)

    return batches


def load_positions(valid_df: pd.DataFrame) -> int:
    """
    Insert all rows from valid_df into demo_schema.trade_positions.
    Uses execute_values in batches of 1,000.  Returns the total number
    of rows actually inserted (rows skipped by ON CONFLICT DO NOTHING
    are NOT counted).

    Reads DB credentials via DB_SECRET_ID environment variable.
    """
    # LOGIC — nothing to insert if valid_df is empty
    if valid_df is None or valid_df.empty:
        logger.info("valid_df is empty; no rows to insert.")
        return 0

    secret_id = os.environ["DB_SECRET_ID"]

    batches = _build_batches(valid_df)
    total_rows = len(valid_df)
    logger.info(
        "Beginning DB load: %d rows in %d batch(es) of up to %d.",
        total_rows,
        len(batches),
        _BATCH_SIZE,
    )

    rows_inserted = 0

    # LOGIC — open a single connection for all batches; commit once on clean exit
    with get_connection(secret_id) as conn:
        with conn.cursor() as cursor:
            for batch_index, batch in enumerate(batches):
                # LOGIC — execute_values is the psycopg2 bulk-insert helper (TAC-6)
                psycopg2.extras.execute_values(
                    cursor,
                    _INSERT_SQL,
                    batch,
                    page_size=_BATCH_SIZE,
                )
                # LOGIC — rowcount reflects rows actually inserted (DO NOTHING rows = 0 contribution)
                batch_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0
                rows_inserted += batch_inserted
                logger.info(
                    "Batch %d/%d: submitted %d rows, inserted %d (DO NOTHING skipped %d).",
                    batch_index + 1,
                    len(batches),
                    len(batch),
                    batch_inserted,
                    len(batch) - batch_inserted,
                )

    logger.info(
        "DB load complete: %d total rows submitted, %d inserted, %d skipped.",
        total_rows,
        rows_inserted,
        total_rows - rows_inserted,
    )
    return rows_inserted