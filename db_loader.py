# BOILERPLATE
import logging
import os
import re

import psycopg2
import psycopg2.extras

import secrets_client

logger = logging.getLogger(__name__)

# LOGIC — batch size for execute_values per design specification
_INSERT_BATCH_SIZE = 500

# LOGIC — idempotent insert SQL using composite PK conflict target
_INSERT_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — regex to parse psycopg2 statusmessage after execute_values, e.g. "INSERT 0 70"
_STATUS_RE = re.compile(r"^INSERT\s+\d+\s+(\d+)$")


def _get_db_connection(credentials: dict):
    # BOILERPLATE — open psycopg2 connection from retrieved secret
    return psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        user=credentials["username"],
        password=credentials["password"],
        dbname=credentials["dbname"],
        connect_timeout=10,
    )


def _parse_rows_inserted(statusmessage: str) -> int:
    # LOGIC — extract inserted count from psycopg2 cursor.statusmessage
    # For ON CONFLICT DO NOTHING, psycopg2 reports only actually-inserted rows
    # statusmessage format: "INSERT 0 <n>" where n == rows actually written
    match = _STATUS_RE.match((statusmessage or "").strip())
    if match:
        return int(match.group(1))
    logger.warning(
        "Could not parse rows_inserted from statusmessage: '%s'; defaulting to 0",
        statusmessage,
    )
    return 0


def _build_row_tuples(valid_df) -> list[tuple]:
    # LOGIC — convert DataFrame rows to tuples in column order matching INSERT
    rows = []
    for _, row in valid_df.iterrows():
        rows.append((
            str(row["trade_id"]),
            str(row["desk_code"]),
            row["trade_date"],          # datetime.date — psycopg2 handles natively
            str(row["instrument_type"]),
            row["notional_amount"],      # Decimal — psycopg2 handles natively
            str(row["currency"]),
            str(row["counterparty_id"]),
        ))
    return rows


def load_positions(valid_df) -> int:
    # LOGIC — short-circuit if nothing to insert
    if valid_df is None or valid_df.empty:
        logger.info("load_positions: empty DataFrame — skipping DB insert, returning 0")
        return 0

    # BOILERPLATE — retrieve credentials from Secrets Manager at runtime
    credentials = secrets_client.get_secret(os.environ["DB_SECRET_ID"])

    row_tuples = _build_row_tuples(valid_df)
    total_attempted = len(row_tuples)

    conn = None
    cursor = None
    try:
        conn = _get_db_connection(credentials)
        cursor = conn.cursor()

        # LOGIC — batched idempotent insert; execute_values handles chunking
        psycopg2.extras.execute_values(
            cursor,
            _INSERT_SQL,
            row_tuples,
            page_size=_INSERT_BATCH_SIZE,
        )

        # LOGIC — parse rows actually inserted from statusmessage
        rows_inserted = _parse_rows_inserted(cursor.statusmessage)

        conn.commit()
        logger.info(
            "load_positions: attempted=%d inserted=%d skipped_duplicate=%d",
            total_attempted,
            rows_inserted,
            total_attempted - rows_inserted,
        )
        return rows_inserted

    except Exception:
        logger.exception("load_positions: exception during DB insert; rolling back")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                logger.exception("load_positions: rollback also failed")
        raise

    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:  # noqa: BLE001
                logger.exception("load_positions: failed to close cursor")
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                logger.exception("load_positions: failed to close connection")