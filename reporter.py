# BOILERPLATE
import io
import json
import logging
import math
from datetime import datetime
from typing import Optional

import pandas
import pytz

logger = logging.getLogger(__name__)


# LOGIC
def build_report(
    raw_df: pandas.DataFrame,
    valid_df: pandas.DataFrame,
    rejected_df: pandas.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> dict:
    # LOGIC — total row counts
    total_rows = len(raw_df)
    rows_loaded = rows_inserted
    rows_rejected = len(rejected_df)

    # LOGIC — ET processing timestamp formatted as ISO-8601 with offset
    processing_timestamp = processing_ts.strftime("%Y-%m-%dT%H:%M:%S%z")

    # LOGIC — per-desk-code counts from valid rows
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_code_counts = valid_df.groupby("desk_code").size().to_dict()
    else:
        desk_code_counts = {}

    # LOGIC — notional min/max from valid rows only
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_min: Optional[float] = float(valid_df["notional_amount"].min())
        notional_max: Optional[float] = float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates per column (NaN + empty string) across the raw DataFrame
    null_rates: dict = {}
    if total_rows > 0:
        for col in raw_df.columns:
            series = raw_df[col]
            null_count = int(series.isna().sum())
            # LOGIC — count empty strings only for string-typed columns
            try:
                empty_count = int((series == "").sum())
            except TypeError:
                empty_count = 0
            null_rates[col] = (null_count + empty_count) / total_rows
    else:
        for col in raw_df.columns:
            null_rates[col] = 0.0

    # LOGIC — assemble report dict matching the JSON schema in DATA CONTRACTS
    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp,
        "total_rows": total_rows,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built for desk_code=%s trade_date=%s: "
        "total=%d loaded=%d rejected=%d",
        desk_code,
        trade_date,
        total_rows,
        rows_loaded,
        rows_rejected,
    )
    return report


# LOGIC
def write_report(
    s3_client,
    report: dict,
    bucket: str,
    report_prefix: str,
    desk_code: str,
    trade_date: str,
) -> str:
    # LOGIC — S3 key for the report file
    s3_key = f"{report_prefix}{desk_code}_{trade_date}_positions_report.json"

    # LOGIC — serialize report to JSON bytes
    report_json = json.dumps(report, default=_json_default)
    report_bytes = report_json.encode("utf-8")

    # BOILERPLATE — upload to S3 using in-memory buffer
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report written to s3://%s/%s",
        bucket,
        s3_key,
    )
    return s3_key


# LOGIC — custom JSON serializer for float edge cases
def _json_default(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")