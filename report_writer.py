# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — required columns for null rate computation
_NULL_RATE_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


# LOGIC
def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    """
    Compute per-column null rates on the raw DataFrame.

    A value is considered null if it is NaN or an empty/whitespace string.
    Rate = null_count / total_rows, as float in [0.0, 1.0].
    Returns 0.0 for all columns if raw_df is empty.
    """
    total = len(raw_df)
    null_rates: dict = {}

    for col in _NULL_RATE_COLUMNS:
        if total == 0:
            null_rates[col] = 0.0
            continue

        if col not in raw_df.columns:
            # Column missing entirely — every row is null
            null_rates[col] = 1.0
            continue

        # LOGIC — null means NaN or empty/whitespace string (matches TAC-4)
        null_mask = raw_df[col].isna() | (raw_df[col].astype(str).str.strip() == "")
        null_rates[col] = float(null_mask.sum()) / float(total)

    return null_rates


# LOGIC
def _safe_notional_stat(valid_df: pd.DataFrame, stat: str):
    """
    Return min or max of notional_amount as float, or None if valid_df is empty.
    stat must be 'min' or 'max'.
    """
    if valid_df.empty:
        return None

    series = valid_df["notional_amount"]
    value = series.min() if stat == "min" else series.max()

    if pd.isna(value):
        return None

    return float(value)


# LOGIC
def build_report(
    filename: str,
    desk_code: str,
    trade_date_str: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    processing_ts_et: datetime,
) -> dict:
    """
    Compute the post-load summary report dict.

    Parameters
    ----------
    filename          : original S3 key of the input file
    desk_code         : desk code parsed from filename
    trade_date_str    : trade date string (YYYY-MM-DD) parsed from filename
    raw_df            : full raw DataFrame before validation split
    valid_df          : validated rows (types coerced)
    rejected_df       : rejected rows with rejection_reason column
    rows_inserted     : count returned by db_loader.load_positions()
    processing_ts_et  : timezone-aware datetime in America/Toronto

    Returns
    -------
    dict matching the exact report JSON structure from the design.
    """
    # LOGIC — verify timestamp is timezone-aware (TAC-7)
    if processing_ts_et.tzinfo is None:
        raise ValueError("processing_ts_et must be a timezone-aware datetime in America/Toronto")

    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted  # LOGIC — TAC-4

    # LOGIC — by_desk_code counts valid (accepted) rows grouped by desk_code
    if not valid_df.empty and "desk_code" in valid_df.columns:
        by_desk_code = {
            str(k): int(v)
            for k, v in valid_df.groupby("desk_code").size().to_dict().items()
        }
    else:
        by_desk_code = {}

    # LOGIC — notional amount statistics on validated rows
    min_notional = _safe_notional_stat(valid_df, "min")
    max_notional = _safe_notional_stat(valid_df, "max")

    # LOGIC — null rates on raw DataFrame before split
    null_rates = _compute_null_rates(raw_df)

    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "processing_timestamp_et": processing_ts_et.isoformat(),
        "total_rows_received": total_rows_received,
        "rows_successfully_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "by_desk_code": by_desk_code,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
    }

    logger.info(
        "Report built: filename=%s total_rows=%d rows_inserted=%d rows_rejected=%d rows_skipped=%d",
        filename,
        total_rows_received,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return report


# LOGIC
def upload_report(report: dict, bucket: str, desk_code: str, trade_date_str: str) -> str:
    """
    Serialize report dict as JSON and upload to S3.

    S3 key: reports/{desk_code}_{trade_date}_report.json

    Returns the S3 key of the uploaded report.
    """
    # BOILERPLATE
    s3_client = boto3.client("s3")

    # LOGIC — exact key pattern from data contracts
    report_key = f"reports/{desk_code}_{trade_date_str}_report.json"

    # LOGIC — JSON serialization with float-safe default for any residual Decimal values
    def _json_default(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    report_body = json.dumps(report, indent=2, default=_json_default)

    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Report uploaded to s3://%s/%s", bucket, report_key)
    return report_key


# LOGIC
def write_manifest(bucket: str, desk_code: str, trade_date_str: str, report_key: str) -> str:
    """
    Write a manifest JSON file at a predictable S3 key so downstream consumers
    can locate the actual report and error files without guessing.

    Manifest key: manifests/{desk_code}_{trade_date}_manifest.json
    Always overwrites on reprocessing (idempotent).

    Returns the S3 key of the manifest file.
    """
    # BOILERPLATE
    s3_client = boto3.client("s3")

    # LOGIC — exact manifest key pattern from data contracts
    manifest_key = f"manifests/{desk_code}_{trade_date_str}_manifest.json"

    # LOGIC — error file key derived from the same desk_code + trade_date
    error_file_key = f"errors/{desk_code}_{trade_date_str}_errors.csv"

    # LOGIC — generated_at timestamp in America/Toronto (TAC-7)
    et_tz = pytz.timezone("America/Toronto")
    generated_at = datetime.now(et_tz).isoformat()

    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "generated_at_et": generated_at,
        "files": {
            "report": report_key,
            "error_file": error_file_key,
        },
    }

    manifest_body = json.dumps(manifest, indent=2)

    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info("Manifest uploaded to s3://%s/%s", bucket, manifest_key)
    return manifest_key