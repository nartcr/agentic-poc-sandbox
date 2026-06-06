# BOILERPLATE
import io
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

logger = logging.getLogger(__name__)


# LOGIC
def build_report(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processed_at: datetime,
    s3_key_source: str,
) -> dict:
    """Compute the summary report dict from the processing DataFrames.

    Returns a dict with the exact keys specified in the data contract.
    """
    # LOGIC — core row counts
    total_rows_received = len(raw_df)
    rows_loaded = rows_inserted
    rows_rejected = len(rejected_df) if rejected_df is not None else 0
    rows_skipped_duplicate_db = (
        len(valid_df) - rows_inserted if valid_df is not None else 0
    )

    # LOGIC — counts_by_desk_code from raw values before validation
    counts_by_desk_code: dict = {}
    if "desk_code" in raw_df.columns and len(raw_df) > 0:
        grouped = raw_df.groupby("desk_code").size()
        counts_by_desk_code = {str(k): int(v) for k, v in grouped.items()}

    # LOGIC — notional_amount min/max from valid_df cast to Decimal
    notional_amount_min = "0.00"
    notional_amount_max = "0.00"
    if valid_df is not None and len(valid_df) > 0 and "notional_amount" in valid_df.columns:
        decimals = []
        for raw_val in valid_df["notional_amount"]:
            try:
                decimals.append(Decimal(str(raw_val)))
            except (InvalidOperation, TypeError):
                # LOGIC — skip values that cannot be converted; validator guarantees these are valid
                logger.warning("Could not convert notional_amount value to Decimal: %r", raw_val)
        if decimals:
            notional_amount_min = "{:.2f}".format(min(decimals))
            notional_amount_max = "{:.2f}".format(max(decimals))

    # LOGIC — null rates per column across raw_df (null = pd.isna OR empty string "")
    columns_for_null_rate = [
        "trade_id",
        "desk_code",
        "trade_date",
        "instrument_type",
        "notional_amount",
        "currency",
        "counterparty_id",
    ]
    null_rates_per_column: dict = {}
    for col in columns_for_null_rate:
        if col in raw_df.columns and len(raw_df) > 0:
            null_count = raw_df[col].apply(
                lambda v: pd.isna(v) or (isinstance(v, str) and v.strip() == "")
            ).sum()
            null_rates_per_column[col] = float(null_count) / float(total_rows_received)
        else:
            # LOGIC — column absent entirely counts as 100% null
            null_rates_per_column[col] = 1.0 if total_rows_received > 0 else 0.0

    # LOGIC — processed_at serialised as ISO-8601 with ET offset (e.g. 2026-06-01T19:45:00-04:00)
    processed_at_iso = processed_at.isoformat()

    report = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_s3_key": s3_key_source,
        "processed_at": processed_at_iso,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate_db": rows_skipped_duplicate_db,
        "counts_by_desk_code": counts_by_desk_code,
        "notional_amount_min": notional_amount_min,
        "notional_amount_max": notional_amount_max,
        "null_rates_per_column": null_rates_per_column,
    }

    logger.info(
        "Report built: desk_code=%s trade_date=%s total=%d loaded=%d rejected=%d skipped=%d",
        desk_code,
        trade_date,
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_duplicate_db,
    )

    return report


# LOGIC
def write_report(
    s3_client,
    report: dict,
    bucket: str,
    report_prefix: str,
    desk_code: str,
    trade_date: str,
    processed_at: datetime,
) -> str:
    """Serialise the report dict to JSON and write it to S3.

    Returns the S3 key of the written report file.
    """
    # LOGIC — build the output S3 key using ET-localised processed_at timestamp
    timestamp_suffix = processed_at.strftime("%Y%m%d%H%M%S")
    s3_key = f"{report_prefix}{desk_code}_{trade_date}_report_{timestamp_suffix}.json"

    # LOGIC — serialise to JSON; indent for human readability, ensure_ascii=False for unicode safety
    report_json = json.dumps(report, ensure_ascii=False, indent=2)
    report_bytes = report_json.encode("utf-8")

    # BOILERPLATE — upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report written: s3://%s/%s",
        bucket,
        s3_key,
    )

    return s3_key