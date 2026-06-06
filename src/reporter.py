# BOILERPLATE
import json
import logging
import math
from datetime import datetime
from io import BytesIO
from typing import Optional

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


# LOGIC
def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
    error_s3_key: Optional[str],
) -> dict:
    """Compute post-load summary statistics and return report dict."""

    # LOGIC — core counts
    total_rows_received = len(raw_df)
    rows_validated = len(valid_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = rows_validated - rows_inserted

    # LOGIC — notional stats (guard against empty valid set)
    if rows_validated > 0:
        notional_min = float(valid_df["notional_amount"].min())
        notional_max = float(valid_df["notional_amount"].max())
        # Guard against NaN (e.g. all-NaN column — shouldn't happen but be safe)
        if math.isnan(notional_min):
            notional_min = None
        if math.isnan(notional_max):
            notional_max = None
    else:
        notional_min = None
        notional_max = None

    # LOGIC — by_desk_code: count of valid rows grouped by desk_code value
    if rows_validated > 0:
        by_desk_code = (
            valid_df.groupby("desk_code", sort=True)
            .size()
            .to_dict()
        )
        # Convert numpy int to Python int for JSON serialisability
        by_desk_code = {k: int(v) for k, v in by_desk_code.items()}
    else:
        by_desk_code = {}

    # LOGIC — null rates across raw_df: empty string and whitespace-only treated as null
    null_rates = {}
    if total_rows_received > 0:
        for col in raw_df.columns:
            series = raw_df[col].astype(str)
            null_count = series.apply(lambda x: x.strip() == "").sum()
            null_rates[col] = float(null_count) / float(total_rows_received)
    else:
        for col in raw_df.columns:
            null_rates[col] = 0.0

    # LOGIC — status
    status = "SUCCESS" if rows_rejected == 0 else "PARTIAL"

    # LOGIC — ISO 8601 timestamp with ET offset
    processing_timestamp_str = processing_ts.isoformat()

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp_str,
        "total_rows_received": total_rows_received,
        "rows_validated": rows_validated,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "rows_rejected": rows_rejected,
        "by_desk_code": by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
        "error_file_s3_key": error_s3_key,
        "status": status,
    }

    logger.info(
        "Report built: desk=%s date=%s status=%s total=%d validated=%d inserted=%d rejected=%d",
        desk_code,
        trade_date,
        status,
        total_rows_received,
        rows_validated,
        rows_inserted,
        rows_rejected,
    )

    return report


# LOGIC
def write_report(
    s3_client,
    bucket: str,
    report: dict,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> str:
    """Serialise report as JSON and upload to S3. Returns the S3 key."""

    # LOGIC — build S3 key with yyyymmddHHMMSS timestamp suffix
    ts_suffix = processing_ts.strftime("%Y%m%d%H%M%S")
    s3_key = f"reports/{desk_code}_{trade_date}_{ts_suffix}_summary.json"

    # LOGIC — serialise to JSON bytes
    json_bytes = json.dumps(report, indent=2).encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, s3_key)
    return s3_key