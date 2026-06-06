# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
import pytz

from config import Config

logger = logging.getLogger(__name__)

# LOGIC — required columns for null_rate computation, per data contract
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _extract_desk_and_date_from_key(source_file_key: str) -> tuple:
    # LOGIC — parse desk_code and trade_date from filename stem
    # Expected filename pattern: {prefix}{desk_code}_{trade_date}_positions.csv
    basename = os.path.basename(source_file_key)  # e.g. EQTY_2026-06-15_positions.csv
    # Remove the _positions.csv suffix
    stem = basename.replace("_positions.csv", "")  # e.g. EQTY_2026-06-15
    # desk_code is everything before the first date segment (YYYY-MM-DD)
    # trade_date is the 10-char ISO date at the end of stem
    # stem format: {desk_code}_{YYYY-MM-DD}
    trade_date = stem[-10:]       # last 10 chars: YYYY-MM-DD
    desk_code = stem[: -(10 + 1)]  # everything before the underscore preceding the date
    return desk_code, trade_date


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    source_file: str,
    load_timestamp_et: datetime,
) -> dict:
    # LOGIC — compute all report fields per data contract
    logger.info("Building summary report for source_file=%s", source_file)

    # Basic counts
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)

    # Min/max notional — null when no valid rows
    if len(valid_df) > 0:
        min_notional: Optional[float] = float(valid_df["notional_amount"].min())
        max_notional: Optional[float] = float(valid_df["notional_amount"].max())
    else:
        min_notional = None
        max_notional = None

    # Record counts by desk_code from valid rows
    if len(valid_df) > 0:
        record_counts_by_desk_code = {
            str(k): int(v)
            for k, v in valid_df.groupby("desk_code").size().to_dict().items()
        }
    else:
        record_counts_by_desk_code = {}

    # Null rates for all 7 required columns, computed against raw_df
    null_rates = {}
    for col in _REQUIRED_COLUMNS:
        if col in raw_df.columns:
            # LOGIC — null rate includes empty-string as non-null (isnull only catches NaN/None)
            null_rates[col] = float(raw_df[col].isnull().mean())
        else:
            # Column entirely absent — 100% null rate
            null_rates[col] = 1.0

    # Rejection reasons summary
    if len(rejected_df) > 0 and "rejection_reason" in rejected_df.columns:
        rejection_reasons_summary = {
            str(k): int(v)
            for k, v in rejected_df["rejection_reason"].value_counts().to_dict().items()
        }
    else:
        rejection_reasons_summary = {}

    report = {
        "source_file": source_file,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "load_timestamp": load_timestamp_et.isoformat(),
        "record_counts_by_desk_code": record_counts_by_desk_code,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
        "rejection_reasons_summary": rejection_reasons_summary,
    }

    logger.info(
        "Report built: total=%d loaded=%d rejected=%d",
        total_rows_received,
        rows_inserted,
        rows_rejected,
    )
    return report


def write_report(
    bucket: str,
    reports_prefix: str,
    report: dict,
    source_file_key: str,
) -> str:
    # LOGIC — derive filename components and upload JSON report to S3
    desk_code, trade_date = _extract_desk_and_date_from_key(source_file_key)

    # LOGIC — ET timestamp for filename
    et_tz = pytz.timezone("America/Toronto")
    run_timestamp = datetime.now(et_tz).strftime("%Y%m%d_%H%M%S")

    # LOGIC — construct S3 key per data contract pattern
    report_key = (
        f"{reports_prefix}{desk_code}_{trade_date}_positions_report_{run_timestamp}.json"
    )

    # LOGIC — serialize report to JSON bytes
    report_body = json.dumps(report, indent=2, default=str).encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client = boto3.client("s3", region_name=Config.AWS_REGION)
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body,
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, report_key)
    return report_key