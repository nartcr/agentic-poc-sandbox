# BOILERPLATE
import json
import logging
import os
import io
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
) -> dict:
    """Constructs the full processing summary report dict from pipeline DataFrames."""
    # LOGIC — core count fields
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — guard: skipped cannot be negative (e.g. if rowcount behaves unexpectedly)
    if rows_skipped_duplicate < 0:
        logger.warning(
            "rows_skipped_duplicate computed as negative (%d); clamping to 0",
            rows_skipped_duplicate,
        )
        rows_skipped_duplicate = 0

    # LOGIC — record counts by desk from raw_df
    if total_rows_received > 0 and "desk_code" in raw_df.columns:
        record_counts_by_desk = (
            raw_df["desk_code"].value_counts().to_dict()
        )
        # LOGIC — ensure all keys are plain strings (not numpy types)
        record_counts_by_desk = {str(k): int(v) for k, v in record_counts_by_desk.items()}
    else:
        record_counts_by_desk = {}

    # LOGIC — notional min/max on valid rows only
    if len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        min_notional_amount = float(notional_series.min())
        max_notional_amount = float(notional_series.max())
    else:
        min_notional_amount = None
        max_notional_amount = None

    # LOGIC — null rates computed on raw_df before any split
    tracked_columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]
    null_rates: dict = {}
    if total_rows_received == 0:
        null_rates = {col: 0.0 for col in tracked_columns}
    else:
        for col in tracked_columns:
            if col in raw_df.columns:
                null_count = int(raw_df[col].isna().sum())
                null_rates[col] = null_count / total_rows_received
            else:
                # LOGIC — column entirely absent treated as 100% null
                null_rates[col] = 1.0

    # LOGIC — processing timestamp in ET
    et_zone = pytz.timezone("America/Toronto")
    processing_timestamp = datetime.now(et_zone).isoformat()

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "record_counts_by_desk": record_counts_by_desk,
        "min_notional_amount": min_notional_amount,
        "max_notional_amount": max_notional_amount,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built for desk_code=%s trade_date=%s: "
        "total=%d loaded=%d rejected=%d skipped=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return report


# LOGIC
def write_report(report: dict, desk_code: str, trade_date: str, bucket: str) -> str:
    """Serializes the report dict to JSON and uploads it to S3. Returns the S3 key written."""
    # LOGIC — construct S3 key per data contract
    s3_key = f"reports/{desk_code}_{trade_date}_positions_report.json"

    # LOGIC — serialize to JSON; use default=str to handle any edge-case non-serializable types
    report_json = json.dumps(report, indent=2, default=str)
    report_bytes = report_json.encode("utf-8")

    # BOILERPLATE — S3 client created at call time; no module-level client
    s3_client = boto3.client("s3")

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report written to s3://%s/%s (%d bytes)",
        bucket,
        s3_key,
        len(report_bytes),
    )

    return s3_key