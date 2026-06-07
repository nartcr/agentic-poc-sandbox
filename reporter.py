# BOILERPLATE
import json
import logging
import boto3
import pandas as pd
import pytz
from datetime import datetime

from config import config

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")

REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    # LOGIC: compute fraction of null-or-empty values per required column on the raw frame
    rates = {}
    total = len(raw_df)
    for col in REQUIRED_COLUMNS:
        if col not in raw_df.columns:
            # entire column absent — treat every row as null
            rates[col] = 1.0 if total > 0 else 0.0
        else:
            if total == 0:
                rates[col] = 0.0
            else:
                null_count = raw_df[col].isna().sum() + (raw_df[col] == "").sum()
                rates[col] = float(null_count) / float(total)
    return rates


def _compute_notional_stats(valid_df: pd.DataFrame):
    # LOGIC: return (min, max) of notional_amount as floats, or (None, None) if frame is empty
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    amounts = valid_df["notional_amount"].astype(float)
    return float(amounts.min()), float(amounts.max())


def _compute_row_counts_by_desk(valid_df: pd.DataFrame) -> dict:
    # LOGIC: count valid rows per desk_code
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    counts = valid_df["desk_code"].value_counts()
    return {str(k): int(v) for k, v in counts.items()}


def build_and_write_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    bucket: str,
    desk_code: str,
    trade_date: str,
    source_key: str,
) -> dict:
    # LOGIC: assemble all report fields, write to S3, return the dict

    processing_timestamp_et = datetime.now(ET).isoformat()

    total_rows_received = len(raw_df)
    rows_validated = len(valid_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = rows_validated - rows_inserted

    notional_min, notional_max = _compute_notional_stats(valid_df)
    row_counts_by_desk = _compute_row_counts_by_desk(valid_df)
    null_rates = _compute_null_rates(raw_df)

    report_s3_key = f"reports/{desk_code}_{trade_date}_positions_report.json"

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file": source_key,
        "processing_timestamp_et": processing_timestamp_et,
        "total_rows_received": total_rows_received,
        "rows_validated": rows_validated,
        "rows_inserted": rows_inserted,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "rows_rejected": rows_rejected,
        "row_counts_by_desk_code": row_counts_by_desk,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates_per_column": null_rates,
        "report_s3_key": report_s3_key,
    }

    # BOILERPLATE: write JSON to S3
    s3_client = boto3.client("s3", region_name=config.aws_region)
    report_body = json.dumps(report, indent=2, default=str)

    logger.info(
        "Writing report to s3://%s/%s", bucket, report_s3_key
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=report_s3_key,
        Body=report_body.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Report written: total_rows=%d validated=%d inserted=%d rejected=%d skipped_duplicate=%d",
        total_rows_received,
        rows_validated,
        rows_inserted,
        rows_rejected,
        rows_skipped_duplicate,
    )

    return report