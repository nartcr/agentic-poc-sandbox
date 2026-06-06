# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

# LOGIC
_MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

_ET = pytz.timezone("America/Toronto")


def build_and_store_report(
    s3_bucket: str,
    source_s3_key: str,
    desk_code: str,
    trade_date: str,
    raw_df,
    valid_df,
    rejected_df,
    rows_inserted: int,
) -> dict:
    # LOGIC — compute all summary statistics
    total_rows = len(raw_df)
    rows_loaded = rows_inserted
    rows_rejected = len(rejected_df)
    processing_timestamp = datetime.now(_ET).isoformat()

    # LOGIC — desk_code_counts: count of valid rows grouped by desk_code
    if not valid_df.empty:
        desk_code_counts = (
            valid_df.groupby("desk_code")["desk_code"]
            .count()
            .to_dict()
        )
    else:
        desk_code_counts = {}

    # LOGIC — min/max notional; default to 0.0 when valid_df is empty
    if not valid_df.empty:
        min_notional = float(valid_df["notional_amount"].min())
        max_notional = float(valid_df["notional_amount"].max())
    else:
        min_notional = 0.0
        max_notional = 0.0

    # LOGIC — null_rates: fraction of rows where the stripped value is empty
    null_rates = {}
    for col in _MANDATORY_COLUMNS:
        if col in raw_df.columns:
            empty_count = raw_df[col].apply(lambda x: str(x).strip() == "").sum()
            null_rates[col] = round(float(empty_count) / float(total_rows), 6)
        else:
            # Column absent entirely — treat as 100% null
            null_rates[col] = 1.0

    report_dict = {
        "source_file": source_s3_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp,
        "total_rows": total_rows,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "desk_code_counts": desk_code_counts,
        "min_notional": min_notional,
        "max_notional": max_notional,
        "null_rates": null_rates,
    }

    # LOGIC — derive S3 key for the report file
    report_key = f"reports/{desk_code}_{trade_date}_summary.json"

    # LOGIC — serialize and write to S3
    json_bytes = json.dumps(report_dict, indent=2).encode("utf-8")

    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=report_key,
            Body=json_bytes,
            ContentType="application/json",
        )
        logger.info(
            "Report written to s3://%s/%s",
            s3_bucket,
            report_key,
        )
    except Exception as exc:
        from src.ingestion.exceptions import ReportWriteError
        raise ReportWriteError(
            f"Failed to write report to s3://{s3_bucket}/{report_key}: {exc}"
        ) from exc

    return report_dict