# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — batch size mandated by TAC-6 to use execute_values with 1,000-row batches
_BATCH_SIZE = 1_000

# LOGIC — target table in the approved schema
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id, loaded_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df: pd.DataFrame, conn: psycopg2.extensions.connection) -> int:
    # LOGIC — nothing to insert when valid_df is empty
    if valid_df.empty:
        logger.info("valid_df is empty — no rows to load into demo_schema.trade_positions.")
        return 0

    # LOGIC — capture a single ET timestamp for all rows in this invocation (TAC-7)
    et_zone = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_zone)

    total_inserted = 0

    with conn.cursor() as cursor:
        # LOGIC — iterate in explicit 1,000-row batches (TAC-6)
        for batch_start in range(0, len(valid_df), _BATCH_SIZE):
            batch = valid_df.iloc[batch_start : batch_start + _BATCH_SIZE]

            # LOGIC — build list of value tuples matching the INSERT column order
            rows = [
                (
                    str(row["trade_id"]),
                    str(row["desk_code"]),
                    str(row["trade_date"]),
                    str(row["instrument_type"]),
                    float(row["notional_amount"]),
                    str(row["currency"]),
                    str(row["counterparty_id"]),
                    loaded_at,
                )
                for _, row in batch.iterrows()
            ]

            psycopg2.extras.execute_values(
                cursor,
                _INSERT_SQL,
                rows,
                page_size=_BATCH_SIZE,
            )

            # LOGIC — rowcount reflects only the rows actually inserted (DO NOTHING skips not counted)
            batch_inserted = cursor.rowcount if cursor.rowcount != -1 else 0
            total_inserted += batch_inserted

            logger.info(
                "Batch %d–%d: %d rows submitted, %d rows inserted.",
                batch_start,
                batch_start + len(batch) - 1,
                len(batch),
                batch_inserted,
            )

    # LOGIC — single commit after all batches to keep the operation atomic
    conn.commit()

    logger.info(
        "load_positions complete: %d total rows inserted into demo_schema.trade_positions "
        "(valid_df had %d rows; %d skipped as duplicates).",
        total_inserted,
        len(valid_df),
        len(valid_df) - total_inserted,
    )

    return total_inserted