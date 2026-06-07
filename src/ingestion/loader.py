# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

logger = logging.getLogger(__name__)

# LOGIC — column order must match the INSERT statement
_INSERT_COLUMNS = [
    'trade_id',
    'desk_code',
    'trade_date',
    'instrument_type',
    'notional_amount',
    'currency',
    'counterparty_id',
]

_BATCH_SIZE = 1000

# LOGIC — idempotent insert; duplicates on (trade_id, desk_code, trade_date) are silently skipped
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type,
     notional_amount, currency, counterparty_id, loaded_at)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def load_positions(valid_df, conn) -> int:
    # LOGIC — batch-insert valid rows; return count of rows actually inserted
    if valid_df.empty:
        logger.info('load_positions called with empty DataFrame; nothing to insert')
        return 0

    et_tz = pytz.timezone('America/Toronto')
    loaded_at = datetime.now(et_tz)

    rows_inserted = 0

    # LOGIC — build list of tuples in column order, appending loaded_at
    all_rows = [
        (
            str(row['trade_id']),
            str(row['desk_code']),
            row['trade_date'],
            str(row['instrument_type']),
            float(row['notional_amount']),
            str(row['currency']).strip(),
            str(row['counterparty_id']),
            loaded_at,
        )
        for _, row in valid_df.iterrows()
    ]

    with conn.cursor() as cursor:
        # LOGIC — process in batches of _BATCH_SIZE for TAC-6 performance
        for batch_start in range(0, len(all_rows), _BATCH_SIZE):
            batch = all_rows[batch_start: batch_start + _BATCH_SIZE]
            psycopg2.extras.execute_values(
                cursor,
                _INSERT_SQL,
                batch,
                template=None,
                page_size=_BATCH_SIZE,
            )
            # LOGIC — rowcount reflects only rows inserted; ON CONFLICT DO NOTHING rows are excluded
            batch_inserted = cursor.rowcount if cursor.rowcount != -1 else 0
            rows_inserted += batch_inserted
            logger.debug(
                'Batch %d–%d: %d rows inserted',
                batch_start,
                batch_start + len(batch) - 1,
                batch_inserted,
            )

    conn.commit()
    logger.info(
        'load_positions complete: %d rows submitted, %d rows inserted',
        len(all_rows),
        rows_inserted,
    )
    return rows_inserted