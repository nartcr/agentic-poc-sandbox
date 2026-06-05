# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import botocore.exceptions
import pandas as pd
import pytz

from exceptions import ReportWriteError
import config

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def build_and_write_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    rows_inserted: int,
    source_file: str,
) -> dict:
    """
    Compute summary statistics and write a JSON report to S3.

    Returns the report dict (used by notifier.py).
    Raises ReportWriteError on S3 failure.
    """
    # LOGIC — compute total rows
    total_rows_received = len(raw_df)

    # LOGIC — compute notional min/max (null if valid_df is empty)
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_min = float(valid_df["notional_amount"].min())
        notional_max = float(valid_df["notional_amount"].max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — compute record counts by desk_code from raw_df
    if "desk_code" in raw_df.columns:
        record_counts_by_desk_code = raw_df["desk_code"].value_counts().to_dict()
    else:
        record_counts_by_desk_code = {}

    # LOGIC — compute null rates for each required field from raw_df
    null_rates: dict[str, float] = {}
    for field in _REQUIRED_FIELDS:
        if field in raw_df.columns and total_rows_received > 0:
            null_count = raw_df[field].apply(
                lambda v: v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""
            ).sum()
            null_rates[field] = int(null_count) / total_rows_received
        else:
            null_rates[field] = 0.0 if total_rows_received == 0 else 1.0

    # LOGIC — build report dict
    report = {
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_inserted,
        "rows_rejected": len(rejected_df),
        "load_timestamp": datetime.now(pytz.timezone("America/Toronto")).isoformat(),
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file": source_file,
        "record_counts_by_desk_code": record_counts_by_desk_code,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates": null_rates,
    }

    # LOGIC — construct S3 key
    report_key = f"{config.S3_REPORTS_PREFIX}{desk_code}_{trade_date}_report.json"

    # LOGIC — serialize to JSON bytes
    json_bytes = json.dumps(report, default=str).encode("utf-8")

    logger.info(
        "Writing report to s3://%s/%s",
        config.S3_BUCKET,
        report_key,
    )

    # LOGIC — upload to S3
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=config.S3_BUCKET, Key=report_key, Body=json_bytes)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise ReportWriteError(
            f"S3 ClientError [{error_code}] writing report '{report_key}'."
        ) from exc
    except Exception as exc:
        raise ReportWriteError(
            f"Unexpected error writing report '{report_key}': {type(exc).__name__}"
        ) from exc

    logger.info("Report written to s3://%s/%s", config.S3_BUCKET, report_key)
    return report