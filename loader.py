# BOILERPLATE
import logging
import io
from datetime import datetime

import psycopg2
import psycopg2.extras
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC
BATCH_SIZE = 1000

# LOGIC
_INSERT_SQL = """
INSERT INTO app.daily_trades
  (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at, source_file)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC
_COUNT_SQL = "SELECT COUNT(*) FROM app.daily_trades WHERE source_file = %s"


def load_trades(conn: psycopg2.extensions.connection, valid_df: pd.DataFrame, source_file: str) -> int:
    """
    Batch-inserts valid trade rows into app.daily_trades.
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING to prevent duplicates.
    Returns the count of rows actually inserted (not attempted).
    """
    # LOGIC: short-circuit if there are no rows to insert
    if valid_df.empty:
        logger.info("load_trades called with empty DataFrame; no rows to insert.")
        return 0

    # LOGIC: compute loaded_at once for the entire batch, in ET
    et_tz = pytz.timezone("America/Toronto")
    loaded_at = datetime.now(et_tz)
    logger.info(
        "Starting load for source_file=%s; row_count=%d; loaded_at=%s",
        source_file,
        len(valid_df),
        loaded_at.isoformat(),
    )

    # LOGIC: capture pre-insert row count for this source_file to compute actual inserts
    with conn.cursor() as cur:
        cur.execute(_COUNT_SQL, (source_file,))
        pre_count = cur.fetchone()[0]
    logger.debug("Pre-insert count for source_file=%s: %d", source_file, pre_count)

    # LOGIC: select only the required columns in the correct order for the INSERT
    required_columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]
    df_to_load = valid_df[required_columns].copy()

    # LOGIC: build list of tuples; append loaded_at and source_file to each row
    rows = [
        (
            row.trade_id,
            row.desk_code,
            row.trade_date,
            row.instrument_type,
            float(row.notional_amount),
            row.currency,
            row.counterparty_id,
            loaded_at,
            source_file,
        )
        for row in df_to_load.itertuples(index=False)
    ]

    # LOGIC: batch-insert in chunks of BATCH_SIZE
    total_batches = 0
    with conn.cursor() as cur:
        for batch_start in range(0, len(rows), BATCH_SIZE):
            batch = rows[batch_start : batch_start + BATCH_SIZE]
            psycopg2.extras.execute_values(cur, _INSERT_SQL, batch, page_size=BATCH_SIZE)
            total_batches += 1
            logger.debug(
                "Inserted batch %d: rows %d–%d of %d",
                total_batches,
                batch_start + 1,
                batch_start + len(batch),
                len(rows),
            )

    # LOGIC: capture post-insert row count to determine actual inserts (handles DO NOTHING correctly)
    with conn.cursor() as cur:
        cur.execute(_COUNT_SQL, (source_file,))
        post_count = cur.fetchone()[0]

    rows_inserted = post_count - pre_count
    logger.info(
        "Load complete for source_file=%s; attempted=%d; inserted=%d; skipped_duplicates=%d",
        source_file,
        len(rows),
        rows_inserted,
        len(rows) - rows_inserted,
    )
    return rows_inserted