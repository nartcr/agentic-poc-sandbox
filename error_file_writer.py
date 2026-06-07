# BOILERPLATE
import io
import logging
from datetime import date

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — fixed column order from data contract: 7 source columns then rejection_reason
_ERROR_CSV_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
    "rejection_reason",
]


def write_error_file(
    s3_client,
    config,
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: date,
) -> str:
    """
    Serialize rejected_df to a UTF-8 CSV and upload to S3 under the errors/ prefix.
    Always writes the file even when rejected_df is empty (header-only CSV).
    Returns the S3 key of the uploaded object.
    """
    # LOGIC — construct the S3 key using the pattern from the data contract
    trade_date_str = trade_date.isoformat()
    s3_key = f"{config.s3_error_prefix}{desk_code}_{trade_date_str}_errors.csv"

    # LOGIC — build a DataFrame with exactly the columns required by the data contract,
    # in the correct order; missing columns default to empty string to avoid KeyError
    # on partially-populated rejection DataFrames
    output_df = _prepare_output_dataframe(rejected_df)

    # LOGIC — serialize to CSV in memory (no filesystem writes, no /tmp/ paths)
    buffer = io.StringIO()
    output_df.to_csv(buffer, index=False, encoding="utf-8")
    csv_bytes = buffer.getvalue().encode("utf-8")

    # BOILERPLATE — upload to S3
    logger.info(
        "write_error_file: uploading %d rejected row(s) to s3://%s/%s",
        len(rejected_df),
        config.s3_bucket,
        s3_key,
    )
    s3_client.put_object(
        Bucket=config.s3_bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )
    logger.info("write_error_file: upload complete — s3_key=%s", s3_key)

    return s3_key


def _prepare_output_dataframe(rejected_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame with exactly the columns in _ERROR_CSV_COLUMNS, in that order.
    If rejected_df is empty, returns an empty DataFrame with those column headers.
    Missing columns are filled with empty strings; extra columns are dropped.
    """
    # LOGIC — empty input: return empty frame with correct headers for confirmation artifact
    if rejected_df.empty:
        return pd.DataFrame(columns=_ERROR_CSV_COLUMNS)

    # LOGIC — reindex to enforce exact column set and order; fill any missing columns
    output_df = rejected_df.reindex(columns=_ERROR_CSV_COLUMNS, fill_value="")
    return output_df