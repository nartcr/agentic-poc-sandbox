# BOILERPLATE
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional

import psycopg2
import psycopg2.extras
import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — target table references (exact schema and table names from infra config)
_POSITIONS_TABLE = "demo_schema.trade_positions"
_AUDIT_TABLE = "demo_schema.pipeline_audit"

# LOGIC — INSERT with idempotent conflict clause on composite PK
_INSERT_POSITIONS_SQL = """
INSERT INTO demo_schema.trade_positions
    (trade_id, desk_code, trade_date, instrument_type, notional_amount, currency, counterparty_id)
VALUES %s
ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
"""

# LOGIC — pre-insert deduplication query: find which keys already exist
_SELECT_EXISTING_SQL = """
SELECT trade_id, desk_code, trade_date
FROM demo_schema.trade_positions
WHERE (trade_id, desk_code, trade_date) IN %s
"""

# LOGIC — audit record insert using exact column names from schema YAML
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (filename, desk_code, trade_date, status, total_rows, rows_inserted,
     rows_rejected, error_message, processing_timestamp_et)
VALUES
    (%(filename)s, %(desk_code)s, %(trade_date)s, %(status)s, %(total_rows)s,
     %(rows_inserted)s, %(rows_rejected)s, %(error_message)s, %(processing_timestamp_et)s)
"""


def _build_key_tuples(valid_df: pd.DataFrame) -> list:
    # LOGIC — extract composite key tuples for deduplication query
    keys = []
    for _, row in valid_df.iterrows():
        keys.append(
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                row["trade_date"],  # datetime.date — psycopg2 handles DATE natively
            )
        )
    return keys


def _count_pre_existing(conn, keys: list) -> int:
    # LOGIC — query for keys that already exist before the insert
    if not keys:
        return 0
    with conn.cursor() as cur:
        # psycopg2 requires a tuple-of-tuples for the IN %s row-constructor pattern
        cur.execute(_SELECT_EXISTING_SQL, (tuple(keys),))
        rows = cur.fetchall()
    count = len(rows)
    logger.debug("Pre-existing row count for batch: %d", count)
    return count


def _df_to_value_tuples(valid_df: pd.DataFrame) -> list:
    # LOGIC — convert DataFrame rows to insertion tuples in column order
    tuples = []
    for _, row in valid_df.iterrows():
        tuples.append(
            (
                str(row["trade_id"]),
                str(row["desk_code"]),
                row["trade_date"],           # datetime.date
                str(row["instrument_type"]),
                row["notional_amount"],      # Decimal — maps to NUMERIC(20,4)
                str(row["currency"]),
                str(row["counterparty_id"]),
            )
        )
    return tuples


def insert_positions(conn, valid_df: pd.DataFrame) -> int:
    """
    # LOGIC — batch upsert valid positions with idempotent conflict handling.

    Algorithm:
    1. Extract composite key set from valid_df
    2. Query pre-existing keys
    3. Execute INSERT ... ON CONFLICT DO NOTHING
    4. Return rows_inserted = len(valid_df) - pre_existing_count
    """
    if valid_df.empty:
        logger.info("No valid rows to insert; skipping insert.")
        return 0

    keys = _build_key_tuples(valid_df)
    pre_existing_count = _count_pre_existing(conn, keys)

    value_tuples = _df_to_value_tuples(valid_df)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            _INSERT_POSITIONS_SQL,
            value_tuples,
            template=None,
            page_size=1000,
        )
    logger.info(
        "INSERT executed: %d candidate rows, %d pre-existing (skipped by ON CONFLICT).",
        len(valid_df),
        pre_existing_count,
    )

    # LOGIC — derive inserted count from pre-insert dedup query
    rows_inserted = len(valid_df) - pre_existing_count
    # Guard: rows_inserted cannot be negative (e.g. concurrent inserts between check and insert)
    rows_inserted = max(rows_inserted, 0)
    logger.info("Rows inserted: %d", rows_inserted)
    return rows_inserted


def write_audit_record(
    conn,
    filename: str,
    desk_code: Optional[str],
    trade_date: Optional[str],
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_timestamp_et: datetime,
) -> None:
    """
    # LOGIC — persist one audit record per pipeline run to demo_schema.pipeline_audit.

    Uses exact column names from infrastructure config YAML.
    trade_date is stored as DATE (None if filename was malformed).
    """
    # LOGIC — parse trade_date string to date object if provided
    trade_date_value: Optional[object] = None
    if trade_date is not None:
        try:
            from datetime import datetime as _dt
            trade_date_value = _dt.strptime(trade_date, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(
                "Could not parse trade_date '%s' for audit record; storing NULL.",
                trade_date,
            )
            trade_date_value = None

    params = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_value,
        "status": status,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    with conn.cursor() as cur:
        cur.execute(_INSERT_AUDIT_SQL, params)

    logger.info(
        "Audit record written: filename=%s status=%s total=%d inserted=%d rejected=%d",
        filename,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
    )