# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET_TZ = pytz.timezone("America/Toronto")

# LOGIC — mandatory columns per data contract
_MANDATORY_COLS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — null rate for each mandatory column on the raw (pre-validation) DataFrame
    rates = {}
    for col in _MANDATORY_COLS:
        if col in raw_df.columns:
            # treat empty strings as null for rate computation
            null_mask = raw_df[col].isnull() | (raw_df[col].astype(str).str.strip() == "")
            rates[col] = round(float(null_mask.mean()), 4)
        else:
            rates[col] = 1.0  # column entirely absent → 100% null
    return rates


def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — min and max notional from valid rows; return (None, None) if no valid rows
    if valid_df.empty:
        return None, None
    notional_series = valid_df["notional_amount"].astype(float)
    min_val = round(float(notional_series.min()), 4)
    max_val = round(float(notional_series.max()), 4)
    return min_val, max_val


def _write_s3_json(bucket: str, key: str, payload: dict) -> None:
    # BOILERPLATE — serialise dict as JSON and upload to S3
    s3_client = boto3.client("s3")
    body = json.dumps(payload, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    logger.info("Wrote JSON to s3://%s/%s", bucket, key)


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    error_s3_key: str | None,
) -> dict:
    """
    Build the post-load summary report, write it to S3, and write the manifest JSON.

    Parameters
    ----------
    raw_df : pd.DataFrame
        Original DataFrame (all rows, before validation).
    valid_df : pd.DataFrame
        Rows that passed all validation checks.
    rejected_df : pd.DataFrame
        Rows that failed at least one validation check.
    desk_code : str
        Desk identifier parsed from the source file key.
    trade_date : str
        Trade date string (``YYYY-MM-DD``) parsed from the source file key.
    rows_inserted : int
        Actual rows inserted by the DB loader (from cursor rowcount sum).
    error_s3_key : str | None
        S3 key of the error CSV written by ``error_file_writer``, or ``None``
        if there were no rejected rows.

    Returns
    -------
    dict
        The full report payload (also written to S3).
    """
    # BOILERPLATE — resolve runtime config
    bucket = os.environ["S3_BUCKET"]

    # LOGIC — current ET timestamp used for both file naming and report field
    now_et = datetime.now(_ET_TZ)
    ts_str = now_et.strftime("%Y%m%dT%H%M%S")
    processing_timestamp_et = now_et.isoformat()

    # LOGIC — core row counts per data contract
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)

    # LOGIC — desk breakdown from valid rows
    counts_by_desk: dict = (
        valid_df.groupby("desk_code").size().to_dict()
        if not valid_df.empty
        else {}
    )
    # convert numpy int64 values to plain Python int for JSON serialisation
    counts_by_desk = {k: int(v) for k, v in counts_by_desk.items()}

    # LOGIC — notional statistics from valid rows
    min_notional, max_notional = _compute_notional_stats(valid_df)

    # LOGIC — null rates computed on raw DataFrame
    null_rates = _compute_null_rates(raw_df)

    # LOGIC — assemble report dict matching data contract JSON structure exactly
    report_key = f"reports/{desk_code}_{trade_date}_report_{ts_str}.json"
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    report: dict = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "counts_by_desk": counts_by_desk,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
        "report_s3_key": report_key,
        "manifest_s3_key": manifest_key,
    }

    # LOGIC — write timestamped report JSON to S3
    _write_s3_json(bucket, report_key, report)
    logger.info(
        "Report written: desk=%s trade_date=%s total=%d loaded=%d rejected=%d",
        desk_code,
        trade_date,
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    # LOGIC — write overwriting manifest JSON at predictable key per data contract
    manifest: dict = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "error_key": error_s3_key,  # null in JSON when None
        "generated_at_et": processing_timestamp_et,
    }
    _write_s3_json(bucket, manifest_key, manifest)
    logger.info("Manifest written: %s", manifest_key)

    return report