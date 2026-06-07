# BOILERPLATE
import json
import logging
from datetime import datetime, date

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — columns used for null-rate calculation (matches data contract)
_NULL_RATE_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


class _SummaryEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles date/datetime objects."""  # BOILERPLATE

    def default(self, obj):  # LOGIC
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


def build_summary(
    raw_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    rows_inserted: int,
    desk_code: str,
    trade_date: str,
    processing_ts: datetime,
) -> dict:
    """
    Compute post-load summary statistics and return as a dict.
    processing_ts must be timezone-aware ET.
    """  # LOGIC

    total_rows_received = len(raw_df)
    rows_loaded = rows_inserted
    rows_rejected = len(rejected_df)
    rows_skipped_duplicate = len(valid_df) - rows_inserted  # LOGIC

    logger.info(
        "Building summary: total=%d loaded=%d rejected=%d skipped=%d",
        total_rows_received,
        rows_loaded,
        rows_rejected,
        rows_skipped_duplicate,
    )

    # LOGIC — notional stats from valid rows only
    if not valid_df.empty and "notional_amount" in valid_df.columns:
        notional_series = valid_df["notional_amount"].astype(float)
        notional_min = float(notional_series.min())
        notional_max = float(notional_series.max())
    else:
        notional_min = None
        notional_max = None

    # LOGIC — record counts by desk_code from raw_df
    if "desk_code" in raw_df.columns and not raw_df.empty:
        record_counts_by_desk_code = (
            raw_df["desk_code"]
            .value_counts()
            .to_dict()
        )
        # Ensure keys are plain Python strings
        record_counts_by_desk_code = {
            str(k): int(v) for k, v in record_counts_by_desk_code.items()
        }
    else:
        record_counts_by_desk_code = {}

    # LOGIC — null rates per column from raw_df, expressed as float [0.0, 1.0]
    null_rates: dict = {}
    for col in _NULL_RATE_COLUMNS:
        if col in raw_df.columns and total_rows_received > 0:
            null_count = int(raw_df[col].isna().sum()) + int(
                (raw_df[col].astype(str).str.strip() == "").sum()
            )
            null_rates[col] = round(null_count / total_rows_received, 6)
        elif total_rows_received == 0:
            null_rates[col] = 0.0
        else:
            # Column absent from raw_df — treat all rows as null
            null_rates[col] = 1.0

    summary = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_ts.isoformat(),
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_skipped_duplicate,
        "record_counts_by_desk_code": record_counts_by_desk_code,
        "notional_amount_min": notional_min,
        "notional_amount_max": notional_max,
        "null_rates": null_rates,
    }

    logger.debug("Summary built: %s", summary)
    return summary


def write_report(
    s3_client,
    bucket: str,
    desk_code: str,
    trade_date: str,
    summary: dict,
) -> str:
    """
    Serialise summary dict to UTF-8 JSON and upload to S3.
    Returns the S3 key of the uploaded report.
    """  # LOGIC

    s3_key = f"reports/{desk_code}_{trade_date}_summary.json"

    json_bytes = json.dumps(summary, cls=_SummaryEncoder, indent=2).encode("utf-8")

    logger.info("Writing report to s3://%s/%s (%d bytes)", bucket, s3_key, len(json_bytes))

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info("Report uploaded successfully: %s", s3_key)
    return s3_key