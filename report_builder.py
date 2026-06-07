# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pandas as pd

from timestamp_helper import format_et

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory fields for null-rate computation (matches data contract)
_MANDATORY_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# BOILERPLATE — error CSV column order matches data contract
_ERROR_CSV_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


# LOGIC
def _timestamp_key_suffix(dt: datetime) -> str:
    """
    Produce the YYYYMMDDTHHMMSS timestamp string used in S3 key names (ET).
    Example: 20260601T210533
    """
    return dt.strftime("%Y%m%dT%H%M%S")


# LOGIC
def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    """
    Compute per-column null rates over the 7 mandatory fields from the raw DataFrame.
    null_rate[col] = fraction of rows where the column value is null/NaN.
    """
    null_rates = {}
    total = len(raw_df)
    for col in _MANDATORY_FIELDS:
        if col in raw_df.columns and total > 0:
            null_rates[col] = float(raw_df[col].isna().mean())
        else:
            null_rates[col] = 0.0
    return null_rates


# LOGIC
def _safe_float(value) -> float:
    """Convert Decimal or numeric to float for JSON serialisation."""
    if value is None:
        return None
    return float(value)


# LOGIC
def build_report(
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    filename: str,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    processing_timestamp_et: datetime,
) -> dict:
    """
    Build the summary report dict.
    Returns the dict (also used as the SNS success payload).
    """
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)

    # LOGIC — rows_by_desk_code from valid_df grouped by desk_code
    if len(valid_df) > 0:
        rows_by_desk_code = valid_df.groupby("desk_code").size().to_dict()
        # Convert any numpy int types to plain Python int for JSON serialisation
        rows_by_desk_code = {k: int(v) for k, v in rows_by_desk_code.items()}
    else:
        rows_by_desk_code = {}

    # LOGIC — notional statistics from valid_df
    if len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        notional_min = _safe_float(valid_df["notional_amount"].min())
        notional_max = _safe_float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates computed over raw_df before any splitting
    null_rates = _compute_null_rates(raw_df)

    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": format_et(processing_timestamp_et),
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_by_desk_code": rows_by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built: total_rows=%d rows_loaded=%d rows_rejected=%d",
        total_rows,
        rows_inserted,
        rows_rejected,
    )
    return report


# LOGIC
def write_report_to_s3(
    report: dict,
    bucket: str,
    desk_code: str,
    trade_date: str,
    timestamp_et: datetime,
) -> str:
    """
    Serialise the report dict to JSON and write it to S3.
    Returns the S3 key of the written report object.
    Key pattern: reports/{desk_code}_{trade_date}_report_{YYYYMMDDTHHMMSS}.json
    """
    suffix = _timestamp_key_suffix(timestamp_et)
    key = f"reports/{desk_code}_{trade_date}_report_{suffix}.json"

    report_json = json.dumps(report, indent=2, default=str)
    report_bytes = report_json.encode("utf-8")

    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s (%d bytes)", bucket, key, len(report_bytes))
    return key


# LOGIC
def write_errors_to_s3(
    rejected_df: pd.DataFrame,
    bucket: str,
    desk_code: str,
    trade_date: str,
    timestamp_et: datetime,
) -> str | None:
    """
    Write rejected rows as a CSV to S3.
    Returns the S3 key if rows were written, or None if rejected_df is empty.
    Key pattern: errors/{desk_code}_{trade_date}_errors_{YYYYMMDDTHHMMSS}.csv
    """
    if rejected_df is None or len(rejected_df) == 0:
        logger.info("No rejected rows — skipping error file write.")
        return None

    suffix = _timestamp_key_suffix(timestamp_et)
    key = f"errors/{desk_code}_{trade_date}_errors_{suffix}.csv"

    # LOGIC — ensure only the contracted columns are written, in the correct order
    output_columns = [c for c in _ERROR_CSV_COLUMNS if c in rejected_df.columns]
    output_df = rejected_df[output_columns].copy()

    # LOGIC — write CSV to an in-memory buffer (no /tmp/ filesystem usage)
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rows, %d bytes)",
        bucket, key, len(output_df), len(csv_bytes),
    )
    return key


# LOGIC
def write_manifest_to_s3(
    bucket: str,
    desk_code: str,
    trade_date: str,
    report_key: str,
    error_key: str | None,
    processing_timestamp_et: datetime,
) -> str:
    """
    Write a manifest JSON to a predictable (non-timestamped) S3 key.
    The manifest is always overwritten on reprocessing so it points to the latest run.
    Key pattern: manifests/{desk_code}_{trade_date}_manifest.json
    Returns the manifest S3 key.
    """
    # LOGIC — predictable key; no timestamp in manifest key itself
    key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "error_key": error_key,  # None serialises to JSON null
        "processing_timestamp_et": format_et(processing_timestamp_et),
    }

    manifest_json = json.dumps(manifest, indent=2, default=str)
    manifest_bytes = manifest_json.encode("utf-8")

    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=manifest_bytes,
        ContentType="application/json",
    )

    logger.info("Manifest written to s3://%s/%s", bucket, key)
    return key