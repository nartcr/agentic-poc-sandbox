# BOILERPLATE
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

import psycopg2
import psycopg2.extras

import secret_manager

logger = logging.getLogger(__name__)

# LOGIC — batch size for execute_values; bounds DB round-trips for large files (TAC-6)
_BATCH_SIZE = 1000

# LOGIC — target table in the data contract
_TARGET_TABLE = "demo_schema.trade_positions"

# LOGIC — insert statement using ON CONFLICT DO NOTHING for idempotency (TAC-3)
_INSERT_SQL = """
    INSERT INTO demo_schema.trade_positions
      (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
    VALUES %s
    ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""


def _coerce_row(row) -> tuple:
    # LOGIC — cast each field to its target DB type before insert
    trade_id = str(row["trade_id"]).strip()
    desk_code = str(row["desk_code"]).strip()

    # LOGIC — parse trade_date string to datetime.date
    trade_date_val = row["trade_date"]
    if isinstance(trade_date_val, date):
        trade_date_coerced = trade_date_val
    else:
        from datetime import datetime as _dt
        trade_date_coerced = _dt.strptime(str(trade_date_val).strip(), "%Y-%m-%d").date()

    instrument_type = str(row["instrument_type"]).strip()

    # LOGIC — parse notional_amount to Decimal for NUMERIC(20,4) precision
    notional_raw = str(row["notional_amount"]).strip()
    try:
        notional_amount = Decimal(notional_raw)
    except InvalidOperation as exc:
        raise ValueError(
            f"Cannot coerce notional_amount to Decimal: {notional_raw!r}"
        ) from exc

    # LOGIC — currency uppercased per design spec (3 chars, alpha)
    currency = str(row["currency"]).strip().upper()

    counterparty_id = str(row["counterparty_id"]).strip()

    return (
        trade_id,
        desk_code,
        trade_date_coerced,
        instrument_type,
        notional_amount,
        currency,
        counterparty_id,
    )


def load_positions(valid_df) -> int:
    # LOGIC — short-circuit: nothing to load
    if valid_df is None or len(valid_df) == 0:
        logger.info("No valid rows to load; skipping DB insert.")
        return 0

    # BOILERPLATE — retrieve credentials at runtime; never from config literals
    credentials = secret_manager.get_db_credentials()

    conn = psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )

    rows_inserted = 0

    try:
        with conn:
            with conn.cursor() as cursor:
                # LOGIC — build the full list of coerced tuples before batching
                all_rows = [_coerce_row(row) for _, row in valid_df.iterrows()]

                total_rows = len(all_rows)
                batch_number = 0

                # LOGIC — iterate in batches of _BATCH_SIZE to bound memory and round-trips
                for batch_start in range(0, total_rows, _BATCH_SIZE):
                    batch = all_rows[batch_start: batch_start + _BATCH_SIZE]
                    batch_number += 1

                    psycopg2.extras.execute_values(
                        cursor,
                        _INSERT_SQL,
                        batch,
                        template=None,
                        page_size=_BATCH_SIZE,
                    )

                    # LOGIC — rowcount reflects only rows actually inserted (ON CONFLICT DO NOTHING
                    # leaves conflicting rows uninserted, so rowcount < len(batch) for duplicates)
                    batch_inserted = cursor.rowcount if cursor.rowcount >= 0 else 0
                    rows_inserted += batch_inserted

                    logger.info(
                        "Batch %d: submitted %d rows, inserted %d rows (cumulative: %d)",
                        batch_number,
                        len(batch),
                        batch_inserted,
                        rows_inserted,
                    )

    except Exception:
        logger.exception("Error loading positions into %s", _TARGET_TABLE)
        raise
    finally:
        conn.close()

    logger.info(
        "Load complete: %d total valid rows submitted, %d rows inserted into %s",
        total_rows,
        rows_inserted,
        _TARGET_TABLE,
    )

    return rows_inserted