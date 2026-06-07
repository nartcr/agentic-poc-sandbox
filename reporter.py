# BOILERPLATE
import json
import logging
import math
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC — mandatory columns for null_rate computation (from DATA CONTRACTS)
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET = pytz.timezone("America/Toronto")


class _ReportEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle numpy numeric types and float NaN/inf from pandas."""  # BOILERPLATE

    def default(self, obj):  # BOILERPLATE
        # Handle numpy integer types
        try:
            import numpy as np  # BOILERPLATE
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                if math.isnan(obj) or math.isinf(obj):
                    return None
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
        except ImportError:
            pass
        return super().default(obj)

    def encode(self, obj):  # BOILERPLATE
        # Intercept float NaN/inf at the top level and in nested structures
        return super().encode(self._sanitize(obj))

    def _sanitize(self, obj):  # BOILERPLATE
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        if isinstance(obj, dict):
            return {k: self._sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._sanitize(v) for v in obj]
        return obj


def build_report(  # LOGIC
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
    source_s3_key: str,
) -> dict:
    """
    Computes post-load summary statistics and returns the report dict.
    Does NOT include error_file_s3_key — caller (pipeline) sets that field
    on the returned dict before passing it to write_report.
    """
    # LOGIC — ensure processing_timestamp is in ET
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    else:
        processing_timestamp = processing_timestamp.astimezone(_ET)

    total_rows_received = len(raw_df)  # LOGIC
    rows_valid = len(valid_df)  # LOGIC
    rows_rejected = len(rejected_df)  # LOGIC
    rows_skipped_duplicate = rows_valid - rows_inserted  # LOGIC

    # LOGIC — desk_code_counts: count by desk_code column in raw_df
    if "desk_code" in raw_df.columns and total_rows_received > 0:
        desk_code_counts = (
            raw_df.groupby("desk_code", dropna=True)["desk_code"]
            .count()
            .to_dict()
        )
        # Convert keys to plain str in case they are numpy/object types
        desk_code_counts = {str(k): int(v) for k, v in desk_code_counts.items()}
    else:
        desk_code_counts = {}

    # LOGIC — notional_min / notional_max: null if no valid rows
    if rows_valid > 0 and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min_val = notional_series.min()
        notional_max_val = notional_series.max()
        notional_min: Optional[float] = None if pd.isna(notional_min_val) else float(notional_min_val)
        notional_max: Optional[float] = None if pd.isna(notional_max_val) else float(notional_max_val)
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null_rates: per-column null rate across raw_df for mandatory columns
    null_rates: dict = {}
    if total_rows_received > 0:
        for col in MANDATORY_COLUMNS:
            if col in raw_df.columns:
                null_count = raw_df[col].isna().sum()
                # Also treat empty strings as null for string columns
                if raw_df[col].dtype == object:
                    null_count = (raw_df[col].isna() | (raw_df[col].astype(str).str.strip() == "")).sum()
                null_rates[col] = float(null_count) / float(total_rows_received)
            else:
                # Column entirely absent — treat all rows as null
                null_rates[col] = 1.0
    else:
        null_rates = {col: 0.0 for col in MANDATORY_COLUMNS}

    # LOGIC — processing_timestamp_et as ISO 8601 string with UTC offset
    processing_timestamp_et_str = processing_timestamp.isoformat()

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_s3_key": source_s3_key,
        "processing_timestamp_et": processing_timestamp_et_str,
        "total_rows_received": total_rows_received,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
        "error_file_s3_key": None,  # LOGIC — pipeline caller overwrites this if errors exist
    }

    logger.info(
        "Report built: desk_code=%s trade_date=%s total=%d valid=%d rejected=%d inserted=%d skipped=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_valid,
        rows_rejected,
        rows_inserted,
        rows_skipped_duplicate,
    )

    return report


def write_report(  # LOGIC
    report: dict,
    bucket: str,
    report_prefix: str,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
) -> str:
    """
    Serializes report dict to JSON and writes it to S3.
    Returns the S3 key of the written report file.
    Key pattern: {report_prefix}{desk_code}_{trade_date}_report_{YYYYMMDDTHHmmss}.json
    """
    # LOGIC — ensure processing_timestamp is in ET for key formatting
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    else:
        processing_timestamp = processing_timestamp.astimezone(_ET)

    # LOGIC — S3 key per DATA CONTRACTS
    s3_key = (
        f"{report_prefix}{desk_code}_{trade_date}"
        f"_report_{processing_timestamp:%Y%m%dT%H%M%S}.json"
    )

    report_json = json.dumps(report, cls=_ReportEncoder, indent=2, ensure_ascii=False)  # LOGIC

    # BOILERPLATE — write to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, s3_key)
    return s3_key