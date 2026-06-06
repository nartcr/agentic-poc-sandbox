# BOILERPLATE
import io
import json
import logging
import os
import posixpath
import datetime
from typing import Optional

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# LOGIC — required columns checked for null rates (data contract: 7 business key + value columns)
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET = pytz.timezone("America/Toronto")


def _derive_stem(source_key: str) -> str:
    # LOGIC — extract filename stem from full S3 key path
    # e.g. "positions/EQTY_2026-06-01_positions.csv" -> "EQTY_2026-06-01_positions"
    basename = posixpath.basename(source_key)
    stem, _ext = os.path.splitext(basename)
    return stem


def build_report(
    source_key: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    load_timestamp: datetime.datetime,
    error_file_key: Optional[str] = None,
) -> dict:
    """
    Compute all summary statistics from the processing run.
    Returns a dict suitable for JSON serialisation.
    """
    # LOGIC — ensure load_timestamp is ET-aware before formatting
    if load_timestamp.tzinfo is None:
        raise ValueError("load_timestamp must be timezone-aware (ET)")

    # LOGIC — convert to ET if supplied in another tz (defensive)
    load_timestamp_et = load_timestamp.astimezone(_ET)

    # LOGIC — core counts
    total_rows_received: int = len(raw_df)
    rows_rejected: int = len(rejected_df)
    rows_loaded: int = rows_inserted
    rows_skipped_duplicate: int = len(valid_df) - rows_inserted

    # LOGIC — desk breakdown from valid rows only
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_code_counts: dict = (
            valid_df.groupby("desk_code")
            .size()
            .to_dict()
        )
        # Ensure values are plain Python ints (not numpy int64)
        desk_code_counts = {k: int(v) for k, v in desk_code_counts.items()}
    else:
        desk_code_counts = {}

    # LOGIC — notional min/max; return None (JSON null) when no valid rows
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_min: Optional[float] = float(valid_df["notional_amount"].min())
        notional_max: Optional[float] = float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates per required column
    # null_rate[col] = (count of NaN + count of empty string) / total_rows
    null_rates: dict = {}
    if total_rows_received > 0:
        for col in _REQUIRED_COLUMNS:
            if col in raw_df.columns:
                nan_count = int(raw_df[col].isna().sum())
                empty_count = int((raw_df[col] == "").sum())
                rate = (nan_count + empty_count) / total_rows_received
                null_rates[col] = round(rate, 6)
            else:
                # Column entirely absent — every row is effectively null
                null_rates[col] = 1.0
    else:
        for col in _REQUIRED_COLUMNS:
            null_rates[col] = 0.0

    # LOGIC — ISO 8601 ET string, e.g. "2026-06-01T20:15:33.123456-04:00"
    load_timestamp_str = load_timestamp_et.isoformat()

    report = {
        "source_file": source_key,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "load_timestamp": load_timestamp_str,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
        "error_file_key": error_file_key if error_file_key else None,
    }

    logger.info(
        "Report built: source=%s total=%d loaded=%d rejected=%d skipped=%d",
        source_key,
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_duplicate,
    )
    return report


def write_report(
    s3_client,
    bucket: str,
    reports_prefix: str,
    report: dict,
    source_key: str,
) -> str:
    """
    Serialise report dict to JSON and upload to S3.
    Returns the full S3 key of the written report.
    """
    # LOGIC — derive report key from source filename stem
    stem = _derive_stem(source_key)
    report_key = f"{reports_prefix}{stem}_report.json"

    # LOGIC — serialise with indent=2, UTF-8 encoded
    json_bytes: bytes = json.dumps(report, indent=2).encode("utf-8")

    # BOILERPLATE — S3 put
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s (%d bytes)", bucket, report_key, len(json_bytes))
    return report_key