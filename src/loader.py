import logging
from datetime import datetime

import psycopg2.extras
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — timezone constant per global rules (all timestamps in ET)
_ET = pytz.timezone("America/Toronto")

# LOGIC — exact INSERT statement per data contract; ON CONFLICT DO NOTHING for idempotency (TAC-3)
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id, loaded_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — columns consumed from valid_df, in the exact order of the INSERT parameter list
_POSITION_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def load_positions(conn, valid_df) -> int:
    """
    Batch-inserts validated trade position rows into demo_schema.trade_positions.

    Uses execute_values for performance (TAC-6: must handle 10 000 rows < 60 s).
    Uses ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING to prevent
    duplicates on reprocessing (TAC-3).

    Returns the count of rows actually inserted (skipped duplicates are excluded).
    """
    # LOGIC — nothing to do if DataFrame is empty
    if valid_df.empty:
        logger.info("valid_df is empty; no rows to load.")
        return 0

    # LOGIC — capture loaded_at once for the entire batch in America/Toronto tz (TAC-7)
    loaded_at: datetime = datetime.now(tz=_ET)

    # LOGIC — build list of tuples in INSERT column order; cast notional_amount to float
    #          to ensure psycopg2 serialises NUMERIC correctly
    rows = []
    for _, row in valid_df[_POSITION_COLUMNS].iterrows():
        rows.append(
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                row["trade_date"],           # kept as-is; psycopg2 handles date/str
                str(row["instrument_type"]),
                float(row["notional_amount"]),
                str(row["currency"]),
                str(row["counterparty_id"]),
                loaded_at,
            )
        )

    logger.info(
        "Loading %d candidate rows into demo_schema.trade_positions (loaded_at=%s).",
        len(rows),
        loaded_at.isoformat(),
    )

    # LOGIC — execute_values performs a single multi-row INSERT for the batch;
    #          page_size controls the maximum VALUES list length per round-trip.
    total_inserted = 0
    with conn.cursor() as cursor:
        # LOGIC — execute_values sends all rows in one statement by default;
        #          page_size splits into batches if the row list is very large.
        psycopg2.extras.execute_values(
            cursor,
            _INSERT_SQL,
            rows,
            template=None,
            page_size=1000,
        )
        # LOGIC — rowcount reflects the actual number of rows inserted after
        #          DO NOTHING filtering; accumulate across implicit pages.
        #          psycopg2 sets rowcount to the total affected rows for execute_values.
        total_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0

    # BOILERPLATE — commit the transaction
    conn.commit()

    logger.info(
        "Committed %d rows inserted into demo_schema.trade_positions "
        "(%d candidates submitted, %d skipped as duplicates).",
        total_inserted,
        len(rows),
        len(rows) - total_inserted,
    )
    return total_inserted