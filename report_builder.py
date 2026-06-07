# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC — 7 business columns whose null rates are reported
_BUSINESS_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
) -> dict:
    """
    Computes and returns a summary report dict.

    Fields:
        total_rows_received         len(raw_df)
        rows_successfully_loaded    rows_inserted  (actual DB inserts)
        rows_rejected               len(rejected_df)
        rows_skipped_duplicate      len(valid_df) - rows_inserted
        processing_timestamp        ET ISO 8601
        desk_code_counts            {desk_code: count} from valid_df
        min_notional                float or None
        max_notional                float or None
        null_rates                  {column: proportion} for 7 business columns
    """
    # LOGIC — core row counts
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — ET timestamp, never UTC
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp = datetime.now(et_tz).isoformat()

    # LOGIC — desk_code_counts from valid rows
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_code_counts = valid_df["desk_code"].value_counts().to_dict()
    else:
        desk_code_counts = {}

    # LOGIC — notional stats; None when no valid rows exist
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        min_notional = float(valid_df["notional_amount"].min())
        max_notional = float(valid_df["notional_amount"].max())
    else:
        min_notional = None
        max_notional = None

    # LOGIC — null rates across the 7 business columns on raw_df
    null_rates: dict = {}
    total = len(raw_df)
    for col in _BUSINESS_COLUMNS:
        if col in raw_df.columns:
            null_count = raw_df[col].isna().sum()
            null_rates[col] = float(null_count) / total if total > 0 else 0.0
        else:
            null_rates[col] = 0.0

    report = {
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "processing_timestamp": processing_timestamp,
        "desk_code_counts": desk_code_counts,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built: total_rows_received=%d rows_successfully_loaded=%d "
        "rows_rejected=%d rows_skipped_duplicate=%d",
        total_rows_received,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return report


def write_report(
    report: dict,
    bucket: str,
    desk_code: str,
    trade_date: str,
) -> str:
    """
    Serialises `report` to JSON and writes it to S3.

    S3 key pattern: reports/{desk_code}_{trade_date}_summary.json

    Returns the S3 key of the written object.
    """
    # LOGIC — S3 key matches the data contract pattern
    s3_key = f"reports/{desk_code}_{trade_date}_summary.json"

    # BOILERPLATE — serialise to JSON bytes
    report_bytes = json.dumps(report, default=str).encode("utf-8")

    # BOILERPLATE — write to S3
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