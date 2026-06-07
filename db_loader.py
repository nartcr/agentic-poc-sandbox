# BOILERPLATE
import logging
import os

import psycopg2.extras

logger = logging.getLogger(__name__)

# LOGIC — batch size per design specification (TAC-6)
_BATCH_SIZE = 500

# LOGIC — target table from infrastructure config
_TABLE = "demo_schema.trade_positions"

# LOGIC — insert column order (loaded_at is DB default)
_INSERT_COLUMNS = (
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
)


def _build_insert_batch(batch: list) -> tuple:
    """
    Build the SQL template string and values list for a batch insert.

    Returns:
        (sql_string, values_list)
        sql_string uses %s placeholder for execute_values()
        values_list is a list of tuples in _INSERT_COLUMNS order
    """
    # LOGIC — construct parameterised INSERT with ON CONFLICT DO NOTHING for idempotency (TAC-3)
    col_list = ", ".join(_INSERT_COLUMNS)
    sql = (
        f"INSERT INTO {_TABLE} ({col_list}) VALUES %s "
        f"ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING"
    )

    # LOGIC — build value tuples in exact column order
    values = []
    for row in batch:
        values.append(
            (
                row["trade_id"],
                row["desk_code"],
                row["trade_date"],
                row["instrument_type"],
                row["notional_amount"],
                row["currency"],
                row["counterparty_id"],
            )
        )

    return (sql, values)


def load_positions(valid_rows: list, db_conn) -> int:
    """
    Batch-insert validated trade position rows into demo_schema.trade_positions.
    Uses ON CONFLICT DO NOTHING for idempotency.

    Args:
        valid_rows: list of typed dicts from row_validator.validate_rows()
        db_conn: live psycopg2 connection (autocommit=False); caller commits

    Returns:
        int — count of net-new rows actually inserted (duplicates excluded)
    """
    # BOILERPLATE
    if not valid_rows:
        logger.info("load_positions called with zero valid rows — nothing to insert")
        return 0

    total_inserted = 0
    total_batches = 0

    # LOGIC — slice into batches of _BATCH_SIZE
    with db_conn.cursor() as cursor:
        for batch_start in range(0, len(valid_rows), _BATCH_SIZE):
            batch = valid_rows[batch_start : batch_start + _BATCH_SIZE]
            sql, values = _build_insert_batch(batch)

            # LOGIC — execute_values sends all rows in one round-trip per batch (TAC-6)
            psycopg2.extras.execute_values(cursor, sql, values)

            # LOGIC — rowcount reflects only net-new inserts (conflicts return 0)
            batch_inserted = cursor.rowcount if cursor.rowcount > 0 else 0
            total_inserted += batch_inserted
            total_batches += 1

            logger.debug(
                "Batch %d: submitted=%d inserted=%d",
                total_batches,
                len(batch),
                batch_inserted,
            )

    logger.info(
        "load_positions complete: batches=%d total_submitted=%d total_inserted=%d",
        total_batches,
        len(valid_rows),
        total_inserted,
    )
    return total_inserted