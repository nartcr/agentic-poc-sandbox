# BOILERPLATE
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — the seven mandatory columns whose null rates must appear in the report
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC — for each mandatory column compute fraction of nulls; default 0.0
    # if the column is entirely absent from the raw DataFrame
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col in raw_df.columns:
            null_rates[col] = float(raw_df[col].isnull().mean())
        else:
            null_rates[col] = 0.0
    return null_rates


def _compute_notional_bounds(valid_df: pd.DataFrame) -> tuple:
    # LOGIC — return (min, max) as Python floats; both None when valid_df is empty
    if len(valid_df) == 0:
        return None, None
    min_val = float(valid_df["notional_amount"].min())
    max_val = float(valid_df["notional_amount"].max())
    return min_val, max_val


def _compute_desk_code_counts(valid_df: pd.DataFrame) -> dict:
    # LOGIC — group valid rows by desk_code and return {desk_code: count} as plain ints
    if len(valid_df) == 0:
        return {}
    return {
        str(k): int(v)
        for k, v in valid_df.groupby("desk_code").size().items()
    }


def _get_et_timestamp() -> str:
    # LOGIC — current timestamp in America/Toronto, formatted as ISO 8601
    et_tz = pytz.timezone("America/Toronto")
    return datetime.now(et_tz).isoformat()


def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date_str: str,
    bucket: str,
    filename: str,
) -> dict:
    # LOGIC — compute all statistics required by the report JSON schema
    total_rows = len(raw_df)
    rows_validated = len(valid_df)
    rows_rejected = len(rejected_df)
    processing_timestamp_et = _get_et_timestamp()
    desk_code_counts = _compute_desk_code_counts(valid_df)
    min_notional, max_notional = _compute_notional_bounds(valid_df)
    null_rates = _compute_null_rates(raw_df)

    report_s3_key = f"reports/{desk_code}_{trade_date_str}_report.json"

    # LOGIC — assemble the full report dict per the JSON schema in the data contracts
    report = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "total_rows": total_rows,
        "rows_validated": rows_validated,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "processing_timestamp_et": processing_timestamp_et,
        "desk_code_counts": desk_code_counts,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
        "report_s3_key": report_s3_key,
    }

    # BOILERPLATE — serialize and upload the report JSON to S3
    report_json = json.dumps(report, indent=2)
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=report_s3_key,
        Body=report_json.encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    logger.info(
        "Report written to s3://%s/%s — total=%d validated=%d inserted=%d rejected=%d",
        bucket,
        report_s3_key,
        total_rows,
        rows_validated,
        rows_inserted,
        rows_rejected,
    )

    return report