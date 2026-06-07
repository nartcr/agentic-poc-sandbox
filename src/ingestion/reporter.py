# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")


def _compute_null_rates(df: pd.DataFrame) -> dict:
    # LOGIC
    if df.empty:
        return {col: 0.0 for col in df.columns}
    total = len(df)
    return {
        col: float(df[col].isna().sum()) / total
        for col in df.columns
    }


def _compute_desk_counts(df: pd.DataFrame) -> dict:
    # LOGIC
    if df.empty or "desk_code" not in df.columns:
        return {}
    return (
        df.groupby("desk_code")
        .size()
        .to_dict()
    )


def build_summary(
    source_key: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    processed_at: datetime,
) -> dict:
    # LOGIC
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — notional stats from valid rows; guard against empty valid_df
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        min_notional = float(notional_series.min())
        max_notional = float(notional_series.max())
    else:
        min_notional = None
        max_notional = None

    summary = {
        "source_file": source_key,
        "processed_at": processed_at.isoformat(),
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_counts": _compute_desk_counts(valid_df),
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": _compute_null_rates(raw_df),
    }

    logger.info(
        "Summary built: source=%s total=%d loaded=%d rejected=%d skipped=%d",
        source_key,
        total_rows_received,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return summary


def write_report(summary: dict, s3_client, bucket: str, report_key: str) -> None:
    # LOGIC
    payload = json.dumps(summary, default=str, indent=2)
    body_bytes = payload.encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=body_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report written to s3://%s/%s (%d bytes)",
        bucket,
        report_key,
        len(body_bytes),
    )