# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex to extract desk_code and trade_date from filenames like EQTY_2026-06-15_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>[A-Z0-9]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$",
    re.IGNORECASE,
)

# LOGIC — columns for which null rates are always computed
_NULL_RATE_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _extract_filename_parts(source_file: str) -> tuple[str, str]:
    """
    # LOGIC
    Extracts desk_code and trade_date string from the source file basename.
    Returns ("UNKNOWN", "UNKNOWN") if the pattern does not match.
    """
    basename = os.path.basename(source_file)
    match = _FILENAME_PATTERN.match(basename)
    if match:
        return match.group("desk_code"), match.group("trade_date")
    logger.warning(
        "source_file '%s' does not match expected naming pattern; "
        "desk_code and trade_date will be 'UNKNOWN'",
        source_file,
    )
    return "UNKNOWN", "UNKNOWN"


def build_report(
    source_file: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    load_timestamp: datetime,
    error_file_key: Optional[str],
) -> dict:
    """
    # LOGIC
    Computes the JSON-serialisable summary report dict from processing results.
    All timestamps are ET.  Returns the dict; does NOT write to S3.
    """
    desk_code, trade_date = _extract_filename_parts(source_file)

    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — notional min/max: None (→ JSON null) when valid_df is empty
    if valid_df.empty:
        notional_min: Optional[float] = None
        notional_max: Optional[float] = None
    else:
        notional_min = float(valid_df["notional_amount"].min())
        notional_max = float(valid_df["notional_amount"].max())

    # LOGIC — desk_code_counts from valid rows only
    if valid_df.empty:
        desk_code_counts: dict = {}
    else:
        desk_code_counts = valid_df["desk_code"].value_counts().to_dict()

    # LOGIC — null_rates: fraction of null/NaN values per named column in raw_df
    null_rates: dict[str, float] = {}
    total = len(raw_df)
    for col in _NULL_RATE_COLUMNS:
        if col in raw_df.columns and total > 0:
            null_rates[col] = round(float(raw_df[col].isna().mean()), 6)
        else:
            null_rates[col] = 0.0

    # LOGIC — load_timestamp must be ET-aware; call isoformat() to embed offset
    load_ts_str = load_timestamp.isoformat()

    report = {
        "source_file": source_file,
        "trade_date": trade_date,
        "desk_code": desk_code,
        "load_timestamp": load_ts_str,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "desk_code_counts": desk_code_counts,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates": null_rates,
        "error_file_key": error_file_key,
    }

    logger.info(
        "Report built for '%s': total=%d loaded=%d rejected=%d skipped=%d",
        source_file,
        total_rows_received,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )
    return report


def write_report(
    report: dict,
    bucket: str,
    source_key: str,
    reports_prefix: str,
) -> str:
    """
    # LOGIC
    Serialises report dict to indented JSON and uploads to
    {reports_prefix}{stem}_report.json.
    Returns the full S3 key written.
    """
    # LOGIC — derive deterministic report key from source key basename
    basename = os.path.basename(source_key)
    stem, _ = os.path.splitext(basename)
    report_key = f"{reports_prefix}{stem}_report.json"

    # BOILERPLATE — serialise and upload
    body = json.dumps(report, indent=2, default=str).encode("utf-8")

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=body,
        ContentType="application/json",
    )

    logger.info("Report uploaded to s3://%s/%s", bucket, report_key)
    return report_key