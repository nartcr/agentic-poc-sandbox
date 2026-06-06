# BOILERPLATE
import json
import logging
from datetime import datetime
from typing import Optional

import boto3
import pandas as pd
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def build_report(
    s3_key: str,
    desk_code: str,
    trade_date: str,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_loaded: int,
    processing_timestamp: datetime,
    error_file_s3_key: Optional[str],
) -> dict:
    # LOGIC
    total_rows_received = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_loaded

    # LOGIC — TAC-4: enforce row count invariant before writing
    assert total_rows_received == rows_loaded + rows_rejected + rows_skipped_duplicate, (
        f"Row count invariant violated: "
        f"total_rows_received={total_rows_received} != "
        f"rows_loaded={rows_loaded} + rows_rejected={rows_rejected} + "
        f"rows_skipped_duplicate={rows_skipped_duplicate}"
    )

    # LOGIC — counts by desk_code from valid_df
    if not valid_df.empty and "desk_code" in valid_df.columns:
        counts_by_desk_code = (
            valid_df.groupby("desk_code").size().to_dict()
        )
        # LOGIC — convert int64 keys/values to native Python int for JSON serialisation
        counts_by_desk_code = {str(k): int(v) for k, v in counts_by_desk_code.items()}
    else:
        counts_by_desk_code = {}

    # LOGIC — min/max notional from valid_df only
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        min_notional = float(valid_df["notional_amount"].min())
        max_notional = float(valid_df["notional_amount"].max())
    else:
        min_notional = None
        max_notional = None

    # LOGIC — null rates computed against raw_df for all 7 mandatory columns
    mandatory_columns = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]
    null_rates = {}
    for col in mandatory_columns:
        if col in raw_df.columns and len(raw_df) > 0:
            null_rates[col] = float(raw_df[col].isnull().mean())
        else:
            null_rates[col] = 0.0

    # LOGIC — ET ISO 8601 timestamp with UTC offset (e.g. -04:00 / -05:00)
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    processing_timestamp_et_str = processing_timestamp.isoformat()

    report = {
        "file_name": s3_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et_str,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "counts_by_desk_code": counts_by_desk_code,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
        "error_file_s3_key": error_file_s3_key,
    }

    logger.info(
        "Report built for s3_key=%s desk_code=%s trade_date=%s "
        "total=%d loaded=%d rejected=%d skipped=%d",
        s3_key,
        desk_code,
        trade_date,
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_duplicate,
    )
    return report


def write_report(
    bucket: str,
    report_prefix: str,
    report: dict,
    desk_code: str,
    trade_date: str,
    processing_timestamp: datetime,
) -> str:
    # LOGIC — format timestamp for S3 key
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)

    ts_str = processing_timestamp.strftime("%Y%m%dT%H%M%S")
    s3_key = f"{report_prefix}{desk_code}_{trade_date}_positions_report_{ts_str}.json"

    # LOGIC — serialise report to JSON with indent=2
    report_json = json.dumps(report, indent=2, default=_json_default)
    report_bytes = report_json.encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info("Report written to s3://%s/%s", bucket, s3_key)
    return s3_key


def _json_default(obj):
    # BOILERPLATE — handle types not natively serialisable by json module
    if isinstance(obj, float) and (obj != obj):
        # NaN
        return None
    if hasattr(obj, "item"):
        # numpy scalar
        return obj.item()
    raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")