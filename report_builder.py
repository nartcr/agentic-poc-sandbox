# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — mandatory columns used for null-rate computation
_MANDATORY_COLUMNS = [
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
    processing_ts: datetime,
    error_file_key: Optional[str] = None,
) -> dict:
    # LOGIC — computes all summary statistics defined in the DATA CONTRACTS
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted

    # LOGIC — counts_by_desk_code from valid_df groupby
    if not valid_df.empty and "desk_code" in valid_df.columns:
        counts_by_desk_code = (
            valid_df.groupby("desk_code").size().to_dict()
        )
        # LOGIC — convert numpy int64 values to plain Python int for JSON serialisation
        counts_by_desk_code = {k: int(v) for k, v in counts_by_desk_code.items()}
    else:
        counts_by_desk_code = {}

    # LOGIC — notional min/max; valid_df has string-typed notional_amount (read with dtype=str)
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        notional_min: Optional[float] = float(notional_series.min())
        notional_max: Optional[float] = float(notional_series.max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null rates per mandatory column over the raw DataFrame
    null_rates: dict = {}
    for col in _MANDATORY_COLUMNS:
        if col in raw_df.columns:
            null_rates[col] = float(raw_df[col].isna().mean())
        else:
            # Column absent entirely — treat as 100% null
            null_rates[col] = 1.0

    report = {
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "processing_timestamp_et": processing_ts.isoformat(),
        "desk_code": desk_code,
        "trade_date": trade_date,
        "counts_by_desk_code": counts_by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
        "error_file_key": error_file_key,
    }

    logger.info(
        "Report built: total_rows=%d rows_loaded=%d rows_rejected=%d rows_skipped_duplicate=%d",
        total_rows,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )
    return report


def write_report(
    report: dict,
    bucket: str,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> str:
    # LOGIC — serialises report to JSON and writes to S3; also writes predictable manifest
    ts_str = processing_ts.strftime("%Y%m%dT%H%M%S")

    report_key = f"reports/{desk_code}_{trade_date}_report_{ts_str}.json"
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    # BOILERPLATE — S3 client; bucket name from parameter (caller passes os.environ["S3_BUCKET"])
    s3_client = boto3.client("s3")

    # LOGIC — write timestamped report JSON
    report_body = json.dumps(report, indent=2, default=str)
    s3_client.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Report written to s3://%s/%s", bucket, report_key)

    # LOGIC — write predictable manifest JSON (overwritten on reprocessing)
    manifest = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_key,
        "error_key": report.get("error_file_key"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
    }
    manifest_body = json.dumps(manifest, indent=2, default=str)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_body.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest written to s3://%s/%s", bucket, manifest_key)

    return report_key