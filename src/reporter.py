# BOILERPLATE
import io
import json
import logging
from datetime import datetime

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE

# LOGIC — the 7 mandatory columns for null-rate computation (matches data contract)
MANDATORY_COLUMNS = [
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
    source_s3_key: str,
    processing_start: datetime,
) -> dict:
    # LOGIC — compute all summary statistics for the processing run
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)

    # LOGIC — rows_skipped_duplicate = valid rows that were not inserted (already existed)
    rows_skipped_duplicate = max(0, len(valid_df) - rows_inserted)

    # LOGIC — notional_min / notional_max from valid rows only; None if no valid rows
    if len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min = notional_series.min()
        notional_max = notional_series.max()
        # LOGIC — convert numpy float to Python float; handle NaN → None
        notional_min = float(notional_min) if pd.notna(notional_min) else None
        notional_max = float(notional_max) if pd.notna(notional_max) else None
    else:
        notional_min = None
        notional_max = None

    # LOGIC — desk_code_counts: frequency of each desk_code value in raw_df
    if "desk_code" in raw_df.columns and len(raw_df) > 0:
        desk_code_counts = (
            raw_df["desk_code"]
            .value_counts()
            .to_dict()
        )
        # LOGIC — convert any numpy int keys/values to plain Python types for JSON serialization
        desk_code_counts = {str(k): int(v) for k, v in desk_code_counts.items()}
    else:
        desk_code_counts = {}

    # LOGIC — null_rates: proportion of null values per mandatory column in raw_df
    null_rates = {}
    for col in MANDATORY_COLUMNS:
        if col in raw_df.columns and total_rows > 0:
            null_count = int(raw_df[col].isnull().sum())
            null_rates[col] = round(null_count / total_rows, 6)
        else:
            # LOGIC — if column is entirely absent from raw data, treat all rows as null
            null_rates[col] = 1.0 if total_rows > 0 else 0.0

    # LOGIC — processing_timestamp_et as ISO 8601 string with UTC offset
    # processing_start must already be timezone-aware (America/Toronto)
    processing_timestamp_et = processing_start.isoformat()

    report = {
        "source_file": source_s3_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et,
        "total_rows_received": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built for desk_code=%s trade_date=%s: total=%d loaded=%d rejected=%d skipped=%d",
        desk_code,
        trade_date,
        total_rows,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return report


def write_report(
    s3_client,
    bucket: str,
    report_prefix: str,
    desk_code: str,
    trade_date: str,
    report: dict,
) -> str:
    # LOGIC — construct S3 key for this report
    s3_key = f"{report_prefix}{desk_code}_{trade_date}_summary.json"

    # LOGIC — serialize report dict to JSON bytes and upload to S3
    report_json = json.dumps(report, indent=2, default=str)
    body_bytes = report_json.encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=io.BytesIO(body_bytes),
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, s3_key)
    return s3_key