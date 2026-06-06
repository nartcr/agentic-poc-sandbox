# BOILERPLATE
import io
import logging
from pathlib import PurePosixPath

import pandas as pd

logger = logging.getLogger(__name__)


def write_error_file(
    s3_client,
    bucket: str,
    errors_prefix: str,
    rejected_df: pd.DataFrame,
    source_key: str,
) -> str:
    """
    Writes the rejected rows DataFrame to a CSV error file in S3 under the errors/ prefix.
    Returns the full S3 key of the written file, or an empty string if there are no rejections.
    """
    # LOGIC — short-circuit: nothing to write if there are no rejected rows
    if rejected_df.empty:
        logger.info(
            "write_error_file: no rejected rows for source_key=%s; skipping S3 write",
            source_key,
        )
        return ""

    # LOGIC — derive the error file key from the source key stem
    # Example: positions/EQTY_2026-06-01_positions.csv
    #       -> errors/EQTY_2026-06-01_positions_errors.csv
    source_path = PurePosixPath(source_key)
    stem = source_path.stem  # filename without extension, e.g. "EQTY_2026-06-01_positions"
    error_filename = f"{stem}_errors.csv"
    # LOGIC — ensure errors_prefix ends with "/" so key is formed correctly
    prefix = errors_prefix if errors_prefix.endswith("/") else f"{errors_prefix}/"
    error_key = f"{prefix}{error_filename}"

    # LOGIC — serialize rejected_df (all original columns + _rejection_reason + _source_row_number) to CSV bytes
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — write to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "write_error_file: wrote %d rejected rows to s3://%s/%s",
        len(rejected_df),
        bucket,
        error_key,
    )

    return error_key