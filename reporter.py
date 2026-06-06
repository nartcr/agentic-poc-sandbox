# BOILERPLATE
import logging
from datetime import datetime

import pandas as pd
import pytz

logger = logging.getLogger(__name__)


# LOGIC
def build_summary(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
) -> dict:
    """
    Build a summary report dict for the current ingestion run.

    Parameters
    ----------
    raw_df        : Original DataFrame as read from S3 (all rows, all columns as str).
    valid_df      : Rows that passed all validation checks, notional_amount cast to float64.
    rejected_df   : Rows that failed one or more validation checks (includes rejection_reason).
    rows_inserted : Actual net-new rows written to the DB (from loader return value).
    desk_code     : Desk code extracted from the S3 filename.
    trade_date    : Trade date string (YYYY-MM-DD) extracted from the S3 filename.

    Returns
    -------
    dict  — all fields documented in the approved design.
    """

    # LOGIC — Eastern Time timestamp for this processing run
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz).isoformat()

    # LOGIC — row count metrics
    total_rows_received: int = len(raw_df)
    rows_validated: int = len(valid_df)
    rows_rejected: int = len(rejected_df)
    rows_skipped_duplicate: int = rows_validated - rows_inserted

    # LOGIC — per-desk breakdown from validated rows
    if not valid_df.empty:
        rows_by_desk_code: dict = (
            valid_df.groupby("desk_code").size().to_dict()
        )
    else:
        rows_by_desk_code = {}

    # LOGIC — notional range from validated rows; None when valid_df is empty
    if not valid_df.empty:
        notional_min: float | None = float(valid_df["notional_amount"].min())
        notional_max: float | None = float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rate per column across the raw DataFrame
    null_rates: dict = {
        col: float(raw_df[col].isna().mean()) for col in raw_df.columns
    }

    summary = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": total_rows_received,
        "rows_validated": rows_validated,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et,
        "rows_by_desk_code": rows_by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info(
        "Summary built — desk_code=%s trade_date=%s total=%d valid=%d "
        "inserted=%d skipped=%d rejected=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_validated,
        rows_inserted,
        rows_skipped_duplicate,
        rows_rejected,
    )

    return summary