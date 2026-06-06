# BOILERPLATE
import logging
from datetime import datetime
from itertools import islice

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

# LOGIC: Maximum rows per INSERT batch to stay within DB parameterisation limits
_BATCH_SIZE = 1000


def _iter_batches(df: pd.DataFrame, batch_size: int):
    # LOGIC: Yield successive DataFrame slices of up to batch_size rows
    start = 0
    total = len(df)
    while start < total:
        yield df.iloc[start : start + batch_size]
        start += batch_size


def load_positions(engine, valid_df: pd.DataFrame, processing_ts: datetime) -> int:
    # LOGIC: Attach loaded_at column (ET-aware processing timestamp) to a working copy
    df = valid_df.copy()
    df["loaded_at"] = processing_ts

    # BOILERPLATE: SQL for idempotent upsert — ON CONFLICT skips pre-existing rows
    insert_sql = text(
        """
        INSERT INTO demo_schema.trade_positions
            (trade_id, desk_code, trade_date, instrument_type,
             notional_amount, currency, counterparty_id, loaded_at)
        VALUES
            (:trade_id, :desk_code, :trade_date, :instrument_type,
             :notional_amount, :currency, :counterparty_id, :loaded_at)
        ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING
        """
    )

    total_inserted = 0

    # LOGIC: Execute inside a single transaction; rollback on any exception
    with engine.begin() as conn:
        try:
            for batch_df in _iter_batches(df, _BATCH_SIZE):
                # LOGIC: Convert each batch to list-of-dicts for SQLAlchemy named params
                rows = []
                for _, row in batch_df.iterrows():
                    rows.append(
                        {
                            "trade_id": str(row["trade_id"]),
                            "desk_code": str(row["desk_code"]),
                            "trade_date": row["trade_date"],
                            "instrument_type": str(row["instrument_type"]),
                            "notional_amount": float(row["notional_amount"]),
                            "currency": str(row["currency"]),
                            "counterparty_id": str(row["counterparty_id"]),
                            "loaded_at": row["loaded_at"],
                        }
                    )

                # LOGIC: Execute batch; rowcount reflects only actually inserted rows
                result = conn.execute(insert_sql, rows)
                batch_inserted = result.rowcount if result.rowcount >= 0 else 0
                total_inserted += batch_inserted

                logger.info(
                    "Batch of %d rows processed; %d inserted (cumulative: %d)",
                    len(rows),
                    batch_inserted,
                    total_inserted,
                )

        except Exception:
            logger.error(
                "Error during load_positions — transaction will be rolled back",
                exc_info=True,
            )
            raise

    logger.info(
        "load_positions complete: %d total rows inserted into demo_schema.trade_positions",
        total_inserted,
    )

    return total_inserted