# BOILERPLATE
import io
import json
import logging
import os
import re
from datetime import datetime
from decimal import Decimal

import boto3
import pandas as pd
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory columns used in null_rate computation
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — regex for parsing incoming source key
_SOURCE_KEY_RE = re.compile(
    r"^incoming/(?P<desk_code>[^_]+(?:_[^_]+)*)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


class _DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder that serialises Decimal as a fixed-4-decimal-place string."""  # BOILERPLATE
    def default(self, obj):  # LOGIC
        if isinstance(obj, Decimal):
            return f"{obj:.4f}"
        return super().default(obj)


def _get_s3_client():  # BOILERPLATE
    return boto3.client("s3")


def _et_now() -> datetime:  # BOILERPLATE
    """Return current datetime in America/Toronto timezone."""
    return datetime.now(pytz.timezone("America/Toronto"))


def _parse_source_key(source_key: str) -> tuple[str, str]:
    """Extract desk_code and trade_date from the incoming S3 key."""  # LOGIC
    match = _SOURCE_KEY_RE.match(source_key)
    if not match:
        raise ValueError(
            f"source_key does not match expected pattern "
            f"'incoming/{{desk_code}}_{{trade_date}}_positions.csv': {source_key!r}"
        )
    return match.group("desk_code"), match.group("trade_date")


def write_summary(
    bucket: str,
    source_key: str,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
) -> str:
    """
    Produce a JSON summary report and write it to S3 under reports/.
    Returns the S3 key of the written report.
    """  # LOGIC
    desk_code, trade_date = _parse_source_key(source_key)

    now_et = _et_now()
    processing_timestamp_et = now_et.isoformat()
    timestamp_str = now_et.strftime("%Y%m%d%H%M%S")

    # LOGIC — row counts
    total_rows = len(valid_df) + len(rejected_df)
    rows_rejected = len(rejected_df)

    # LOGIC — by_desk_code breakdown from valid rows
    if not valid_df.empty and "desk_code" in valid_df.columns:
        by_desk_code = (
            valid_df.groupby("desk_code", sort=True)
            .size()
            .to_dict()
        )
        # Convert numpy int64 to plain int for JSON serialisation
        by_desk_code = {k: int(v) for k, v in by_desk_code.items()}
    else:
        by_desk_code = {}

    # LOGIC — notional min/max (Decimal-safe)
    if not valid_df.empty and "notional_amount" in valid_df.columns and len(valid_df) > 0:
        notional_series = valid_df["notional_amount"]
        notional_min = f"{min(notional_series):.4f}"
        notional_max = f"{max(notional_series):.4f}"
    else:
        notional_min = None
        notional_max = None

    # LOGIC — null_rates across combined raw rows (valid + rejected)
    # Reconstruct a combined view using original string columns from rejected_df
    # and valid_df (cast columns may differ). We compute null/blank rates per
    # mandatory column across the union of all input rows.
    null_rates: dict = {}
    if total_rows > 0:
        # Build a unified series for each mandatory column
        for col in MANDATORY_COLUMNS:
            null_count = 0
            for df_part in (valid_df, rejected_df):
                if col in df_part.columns:
                    # Treat NaN, None, and whitespace-only strings as null
                    col_series = df_part[col].astype(str)
                    null_count += int(
                        col_series.apply(lambda v: v.strip() in ("", "None", "nan")).sum()
                    )
                else:
                    # Column entirely absent — all rows counted as null
                    null_count += len(df_part)
            null_rates[col] = round(null_count / total_rows, 4)
    else:
        null_rates = {col: 0.0 for col in MANDATORY_COLUMNS}

    # LOGIC — assemble summary payload
    report_payload = {
        "filename": source_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et,
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "by_desk_code": by_desk_code,
        "notional_min": notional_min,
        "notional_max": notional_max,
        "null_rates": null_rates,
    }

    # LOGIC — construct S3 key
    report_key = f"reports/{desk_code}_{trade_date}_{timestamp_str}_summary.json"

    # LOGIC — serialise and write to S3
    json_bytes = json.dumps(report_payload, cls=_DecimalEncoder, indent=2).encode("utf-8")
    s3 = _get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Summary report written to s3://%s/%s (total=%d, loaded=%d, rejected=%d)",
        bucket,
        report_key,
        total_rows,
        rows_inserted,
        rows_rejected,
    )
    return report_key


def write_manifest(
    bucket: str,
    desk_code: str,
    trade_date: str,
    report_s3_key: str,
    error_s3_key: str,
) -> str:
    """
    Write a predictable manifest JSON to S3 under manifests/.
    Overwrites any existing manifest for the same desk_code/trade_date (idempotent).
    Returns the S3 key of the manifest.
    """  # LOGIC
    now_et = _et_now()
    generated_at_et = now_et.isoformat()

    # LOGIC — error_key is null when there are no rejected rows
    error_key_value = error_s3_key if error_s3_key else None

    manifest_payload = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "report_key": report_s3_key,
        "error_key": error_key_value,
        "generated_at_et": generated_at_et,
    }

    # LOGIC — predictable key (no timestamp); overwrite on every run
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    json_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")
    s3 = _get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Manifest written to s3://%s/%s (report_key=%r, error_key=%r)",
        bucket,
        manifest_key,
        report_s3_key,
        error_key_value,
    )
    return manifest_key