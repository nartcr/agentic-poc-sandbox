import io
import json
import logging
import os
from datetime import datetime

import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame, rows_received: int) -> dict:
    # LOGIC — null rate = (nulls + empty strings) / rows_received for each mandatory column
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col not in raw_df.columns:
            null_rates[col] = 1.0
            continue
        if rows_received == 0:
            null_rates[col] = 0.0
            continue
        null_count = int(
            raw_df[col].isna().sum() + (raw_df[col].fillna("") == "").sum()
            - (raw_df[col].isna() & (raw_df[col].fillna("") == "")).sum()
        )
        null_rates[col] = null_count / rows_received
    return null_rates


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — min/max on valid rows only; return None if no valid rows
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    notional_series = valid_df["notional_amount"].astype(float)
    return float(notional_series.min()), float(notional_series.max())


def _compute_desk_code_counts(valid_df: pd.DataFrame) -> dict:
    # LOGIC — group valid rows by desk_code
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    return {str(k): int(v) for k, v in valid_df.groupby("desk_code").size().items()}


def generate_report(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    rows_received: int,
    rows_loaded: int,
    source_file: str,
    desk_code: str,
    trade_date: str,
    s3_client,
) -> dict:
    # LOGIC — compute all report fields per data contract
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz).isoformat()

    rows_rejected = len(rejected_df)

    min_notional, max_notional = _compute_notional_stats(valid_df)
    desk_code_counts = _compute_desk_code_counts(valid_df)
    null_rates = _compute_null_rates(raw_df, rows_received)

    report = {
        "source_file": source_file,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et,
        "desk_code_counts": desk_code_counts,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
    }

    # LOGIC — serialize and write report JSON to S3
    s3_bucket = os.environ["S3_BUCKET"]
    s3_report_prefix = os.environ["S3_REPORT_PREFIX"]
    report_key = f"{s3_report_prefix}{desk_code}_{trade_date}_report.json"

    report_json = json.dumps(report, default=str)

    logger.info(
        "Writing summary report to s3://%s/%s", s3_bucket, report_key
    )

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=report_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Report written successfully: source_file=%s rows_received=%d rows_loaded=%d rows_rejected=%d",
        source_file,
        rows_received,
        rows_loaded,
        rows_rejected,
    )

    return report