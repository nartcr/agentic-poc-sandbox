# BOILERPLATE
import json
import logging
import math
from datetime import datetime

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rate(series: pd.Series) -> float:
    # LOGIC — count NaN or whitespace-only string values, divide by length
    if len(series) == 0:
        return 0.0
    null_count = series.apply(
        lambda v: v is None
        or (isinstance(v, float) and math.isnan(v))
        or (isinstance(v, str) and v.strip() == "")
        or pd.isnull(v)
    ).sum()
    return float(null_count) / float(len(series))


def build_and_write_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    bucket: str,
    desk_code: str,
    trade_date: str,
) -> dict:
    # BOILERPLATE — derive computed values
    total_rows = len(raw_df)
    rows_valid = len(valid_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = rows_valid - rows_inserted

    # LOGIC — ET timestamp
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz).isoformat()

    # LOGIC — desk_code_counts from valid rows
    if not valid_df.empty and "desk_code" in valid_df.columns:
        desk_code_counts = valid_df.groupby("desk_code").size().to_dict()
        # convert numpy int64 to plain int for JSON serialisation
        desk_code_counts = {k: int(v) for k, v in desk_code_counts.items()}
    else:
        desk_code_counts = {}

    # LOGIC — notional stats; guard against empty valid_df
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        notional_min = float(notional_series.min())
        notional_max = float(notional_series.max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null_rates computed over raw_df for all 7 mandatory columns
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col in raw_df.columns and total_rows > 0:
            null_rates[col] = _compute_null_rate(raw_df[col])
        else:
            null_rates[col] = 0.0

    # LOGIC — assemble canonical report dict matching data contract schema
    filename = f"{desk_code}_{trade_date}_positions.csv"
    report_s3_key = f"reports/{desk_code}_{trade_date}_positions_report.json"

    summary = {
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et,
        "total_rows": total_rows,
        "rows_valid": rows_valid,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "rows_rejected": rows_rejected,
        "desk_code_counts": desk_code_counts,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
        "report_s3_key": report_s3_key,
    }

    # LOGIC — serialise and write to S3
    report_bytes = json.dumps(summary, indent=2).encode("utf-8")
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=report_s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )
    logger.info(
        "Report written to s3://%s/%s — total_rows=%d rows_inserted=%d rows_rejected=%d",
        bucket,
        report_s3_key,
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    return summary