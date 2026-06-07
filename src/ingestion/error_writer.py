# BOILERPLATE
import io
import logging
import os
import re

import pandas as pd

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — pattern matches incoming/{desk_code}_{trade_date}_positions.csv
_INCOMING_PATTERN = re.compile(
    r"incoming/([A-Z0-9_]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _derive_error_key(source_key: str) -> str:
    # LOGIC
    match = _INCOMING_PATTERN.search(source_key)
    if not match:
        raise ValueError(
            f"Cannot derive error key from source key — "
            f"does not match expected pattern: {source_key}"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return f"errors/{desk_code}_{trade_date}_positions_errors.csv"


def write_error_file(
    rejected_df: pd.DataFrame,
    s3_client,
    bucket: str,
    source_key: str,
) -> str:
    # LOGIC
    error_key = _derive_error_key(source_key)

    # LOGIC — serialize to CSV in memory; no /tmp/ paths used
    buffer = io.StringIO()
    rejected_df.to_csv(buffer, index=False)
    body_bytes = buffer.getvalue().encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=body_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows, %d bytes)",
        bucket,
        error_key,
        len(rejected_df),
        len(body_bytes),
    )

    return error_key