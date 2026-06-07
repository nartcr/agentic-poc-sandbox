# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET_ZONE = pytz.timezone("America/Toronto")


def _compute_null_rates(df: pd.DataFrame) -> dict:
    # LOGIC
    # A value is considered null if it is NaN/None OR empty string after strip.
    total = len(df)
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col not in df.columns:
            null_rates[col] = 1.0
            continue
        if total == 0:
            null_rates[col] = 0.0
            continue
        # Count rows where value is NaN, None, or empty/whitespace string
        null_count = df[col].apply(
            lambda v: pd.isna(v) or (isinstance(v, str) and v.strip() == "")
        ).sum()
        null_rates[col] = float(null_count) / float(total)
    return null_rates


def build_and_publish_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processing_timestamp_et: datetime,
    bucket: str,
    report_prefix: str,
) -> dict:
    # LOGIC — compute all summary statistics
    total_rows_received = len(raw_df)
    rows_valid = len(valid_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = rows_valid - rows_inserted

    # LOGIC — rows_by_desk_code from valid_df
    if rows_valid > 0 and "desk_code" in valid_df.columns:
        rows_by_desk_code = (
            valid_df.groupby("desk_code")
            .size()
            .to_dict()
        )
        # Convert numpy int to plain int for JSON serialization
        rows_by_desk_code = {k: int(v) for k, v in rows_by_desk_code.items()}
    else:
        rows_by_desk_code = {}

    # LOGIC — notional min/max from valid_df
    if rows_valid > 0 and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        notional_min = float(notional_series.min())
        notional_max = float(notional_series.max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates across all 7 mandatory columns in raw_df
    null_rates = _compute_null_rates(raw_df)

    # LOGIC — ET ISO-8601 timestamp
    ts_et = processing_timestamp_et.astimezone(_ET_ZONE)
    processing_timestamp_et_str = ts_et.isoformat()

    # LOGIC — assemble summary dict
    summary = {
        "total_rows_received": total_rows_received,
        "rows_valid": rows_valid,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et_str,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "rows_by_desk_code": rows_by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    # LOGIC — enforce arithmetic identity (log a warning if violated)
    if total_rows_received != rows_valid + rows_rejected:
        logger.warning(
            "Row count identity violated: total_rows_received=%d, "
            "rows_valid=%d, rows_rejected=%d",
            total_rows_received,
            rows_valid,
            rows_rejected,
        )

    # LOGIC — serialize to JSON and upload to S3
    report_json = json.dumps(summary, indent=2)
    prefix = report_prefix.rstrip("/")
    s3_key = f"{prefix}/{desk_code}_{trade_date}_summary.json"

    logger.info(
        "Publishing summary report to s3://%s/%s",
        bucket,
        s3_key,
    )

    # BOILERPLATE — S3 client instantiated locally to avoid module-level side effects
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Summary report published: desk_code=%s trade_date=%s "
        "total=%d valid=%d inserted=%d rejected=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_valid,
        rows_inserted,
        rows_rejected,
    )

    return summary