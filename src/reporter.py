# BOILERPLATE
import io
import json
import logging
import os
import re
from datetime import datetime

import pandas as pd
import pytz

from src.s3_client import upload_bytes

logger = logging.getLogger(__name__)

# LOGIC
_ET = pytz.timezone("America/Toronto")
_FILENAME_RE = re.compile(
    r"^(?:.*/)?([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# Columns for which null rates are computed (full input DataFrame columns per data contract)
_NULL_RATE_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_REPORT_PREFIX = "reports/"


def _parse_desk_and_date_from_key(source_key: str):
    # LOGIC — extract desk_code and trade_date from S3 key filename component
    match = _FILENAME_RE.match(source_key)
    if not match:
        raise ValueError(
            f"Cannot derive desk_code/trade_date from source_key: {source_key!r}"
        )
    return match.group(1), match.group(2)


def build_and_upload_report(
    source_key: str,
    total_rows: int,
    loaded_rows: int,
    rejected_rows: int,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    processing_timestamp: datetime,
    bucket: str,
) -> dict:
    # LOGIC — derive desk_code and trade_date for the S3 report key
    desk_code, trade_date = _parse_desk_and_date_from_key(source_key)

    # LOGIC — ensure processing_timestamp carries ET timezone info
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    else:
        processing_timestamp = processing_timestamp.astimezone(_ET)

    # LOGIC — rows_by_desk_code: count by desk_code in valid_df
    if not valid_df.empty and "desk_code" in valid_df.columns:
        rows_by_desk: dict = (
            valid_df.groupby("desk_code")
            .size()
            .sort_index()
            .to_dict()
        )
        # convert numpy int types to plain Python int for JSON serialisation
        rows_by_desk = {k: int(v) for k, v in rows_by_desk.items()}
    else:
        rows_by_desk = {}

    # LOGIC — notional min/max from valid_df only
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = pd.to_numeric(valid_df["notional_amount"], errors="coerce")
        notional_min = float(notional_series.min()) if notional_series.notna().any() else None
        notional_max = float(notional_series.max()) if notional_series.notna().any() else None
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null_rates_by_column computed over the full input DataFrame
    # Reconstruct full input by concatenating valid and rejected (minus rejection_reason)
    valid_cols = valid_df[_NULL_RATE_COLUMNS] if not valid_df.empty else pd.DataFrame(columns=_NULL_RATE_COLUMNS)
    if not rejected_df.empty:
        rejected_cols = rejected_df[[c for c in _NULL_RATE_COLUMNS if c in rejected_df.columns]]
        # ensure all expected columns present
        for col in _NULL_RATE_COLUMNS:
            if col not in rejected_cols.columns:
                rejected_cols = rejected_cols.copy()
                rejected_cols[col] = None
        rejected_cols = rejected_cols[_NULL_RATE_COLUMNS]
        full_df = pd.concat([valid_cols, rejected_cols], ignore_index=True)
    else:
        full_df = valid_cols

    null_rates: dict = {}
    for col in _NULL_RATE_COLUMNS:
        if col in full_df.columns and len(full_df) > 0:
            rate = float(full_df[col].isna().mean())
        else:
            rate = 0.0
        null_rates[col] = round(rate, 6)

    # LOGIC — assemble report dict matching the specified JSON structure
    report = {
        "source_file": source_key,
        "processing_timestamp": processing_timestamp.isoformat(),
        "total_rows_received": int(total_rows),
        "rows_loaded": int(loaded_rows),
        "rows_rejected": int(rejected_rows),
        "rows_by_desk_code": rows_by_desk,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates_by_column": null_rates,
    }

    # LOGIC — upload JSON report to S3 under reports/ prefix
    report_key = f"{_REPORT_PREFIX}{desk_code}_{trade_date}_report.json"
    report_bytes = json.dumps(report, indent=2).encode("utf-8")

    try:
        upload_bytes(
            bucket=bucket,
            key=report_key,
            data=report_bytes,
            content_type="application/json",
        )
        logger.info("Report uploaded to s3://%s/%s", bucket, report_key)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to upload report to S3: %s", exc)
        raise RuntimeError(f"Report upload failed for key {report_key}") from exc

    return report