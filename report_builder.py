# BOILERPLATE
import json
import logging
from datetime import date, datetime
from io import BytesIO

import pandas as pd

from pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)


# LOGIC
def _compute_null_rates(raw_df: pd.DataFrame) -> dict:
    """
    For each column in raw_df, compute null_count / total_rows_received.
    Null includes both pandas NaN/None and empty strings for object columns.
    Rounded to 4 decimal places.
    """
    total = len(raw_df)
    if total == 0:
        return {col: 0.0 for col in raw_df.columns}

    null_rates = {}
    for col in raw_df.columns:
        series = raw_df[col]
        # LOGIC: count NaN/None
        null_count = int(series.isna().sum())
        # LOGIC: also count empty strings for object/string columns
        if series.dtype == object:
            null_count += int((series.dropna() == "").sum())
        null_rates[col] = round(null_count / total, 4)
    return null_rates


# LOGIC
def _compute_rejection_reasons_summary(rejected_df: pd.DataFrame) -> dict:
    """
    Returns a dict of {rejection_reason: count} from rejected_df.
    Returns empty dict if no rejected rows or no rejection_reason column.
    """
    if rejected_df.empty or "rejection_reason" not in rejected_df.columns:
        return {}
    counts = rejected_df["rejection_reason"].value_counts()
    return {str(reason): int(count) for reason, count in counts.items()}


# LOGIC
def _compute_record_counts_by_desk(valid_df: pd.DataFrame) -> dict:
    """
    Returns {desk_code: row_count} from valid_df grouped by desk_code.
    Returns empty dict if valid_df is empty or desk_code column is absent.
    """
    if valid_df.empty or "desk_code" not in valid_df.columns:
        return {}
    grouped = valid_df.groupby("desk_code").size()
    return {str(k): int(v) for k, v in grouped.items()}


# LOGIC
def _compute_notional_stats(valid_df: pd.DataFrame) -> tuple:
    """
    Returns (min_notional, max_notional) as floats, or (None, None) if no valid rows.
    """
    if valid_df.empty or "notional_amount" not in valid_df.columns:
        return None, None
    min_val = valid_df["notional_amount"].min()
    max_val = valid_df["notional_amount"].max()
    # LOGIC: guard against NaN from an all-null column (should not occur after validation)
    import math
    if pd.isna(min_val) or pd.isna(max_val):
        return None, None
    return float(min_val), float(max_val)


# LOGIC
def _enforce_row_count_invariant(
    total_rows_received: int,
    rows_loaded: int,
    rows_rejected: int,
    rows_skipped_dedup: int,
) -> None:
    """
    TAC-4: total_rows_received == rows_loaded + rows_rejected + rows_skipped_dedup.
    Raises ValueError if the invariant is violated.
    """
    expected = rows_loaded + rows_rejected + rows_skipped_dedup
    if total_rows_received != expected:
        raise ValueError(
            f"Row count invariant violated: total_rows_received={total_rows_received} "
            f"!= rows_loaded({rows_loaded}) + rows_rejected({rows_rejected}) "
            f"+ rows_skipped_dedup({rows_skipped_dedup}) = {expected}"
        )


# LOGIC
def build_and_upload_report(
    s3_client,
    config: PipelineConfig,
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    processing_timestamp: datetime,
    desk_code: str,
    trade_date: date,
) -> dict:
    """
    Computes the processing summary, enforces the row count invariant,
    serializes to JSON, uploads to S3, and returns the report dict.

    S3 key: {S3_REPORT_PREFIX}{desk_code}_{trade_date}_report.json
    e.g.   reports/DESKX_2026-06-15_report.json
    """
    logger.info(
        "Building report for desk_code=%s trade_date=%s", desk_code, trade_date
    )

    # LOGIC: compute summary statistics
    total_rows_received = len(raw_df)
    rows_loaded = rows_inserted
    rows_skipped_dedup = len(valid_df) - rows_inserted
    rows_rejected = len(rejected_df)

    # LOGIC: enforce invariant before writing anything (TAC-4)
    _enforce_row_count_invariant(
        total_rows_received, rows_loaded, rows_rejected, rows_skipped_dedup
    )

    # LOGIC: compute derived statistics
    null_rates = _compute_null_rates(raw_df)
    rejection_reasons_summary = _compute_rejection_reasons_summary(rejected_df)
    record_counts_by_desk = _compute_record_counts_by_desk(valid_df)
    min_notional, max_notional = _compute_notional_stats(valid_df)

    # LOGIC: serialize processing_timestamp as ISO 8601 with ET offset (TAC-7)
    processing_timestamp_str = processing_timestamp.isoformat()

    # LOGIC: construct the full report dict matching the SNS/S3 contract
    report = {
        "status": "SUCCESS",
        "desk_code": desk_code,
        "trade_date": trade_date.isoformat(),
        "processing_timestamp": processing_timestamp_str,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_dedup": rows_skipped_dedup,
        "record_counts_by_desk": record_counts_by_desk,
        "min_notional_amount": min_notional,
        "max_notional_amount": max_notional,
        "null_rates": null_rates,
        "rejection_reasons_summary": rejection_reasons_summary,
        # LOGIC: these keys are populated by pipeline_handler after the fact;
        # report_builder sets placeholders that handler overwrites before SNS publish
        "report_s3_key": f"{config.s3_report_prefix}{desk_code}_{trade_date}_report.json",
        "error_file_s3_key": f"{config.s3_error_prefix}{desk_code}_{trade_date}_errors.csv",
    }

    # LOGIC: serialize to JSON bytes for S3 upload
    report_json = json.dumps(report, indent=2, default=str)
    report_bytes = report_json.encode("utf-8")

    # LOGIC: construct S3 key using infrastructure config values
    s3_key = f"{config.s3_report_prefix}{desk_code}_{trade_date}_report.json"

    logger.info("Uploading report to s3://%s/%s", config.s3_bucket, s3_key)

    # BOILERPLATE: upload report JSON to S3
    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=s3_key,
        Body=report_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Report uploaded successfully: s3://%s/%s "
        "(total=%d loaded=%d rejected=%d skipped_dedup=%d)",
        config.s3_bucket,
        s3_key,
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_dedup,
    )

    return report