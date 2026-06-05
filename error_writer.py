# BOILERPLATE
import logging

import boto3
import botocore.exceptions
import pandas as pd

from exceptions import ErrorWriteError
import config

logger = logging.getLogger(__name__)


def write_error_file(
    rejected_df: pd.DataFrame,
    desk_code: str,
    trade_date: str,
    source_file: str,
) -> str | None:
    """
    Write the rejected-row DataFrame as a CSV error file to S3 errors/ prefix.

    Returns the full S3 key of the written error file, or None if no rows were rejected.
    Raises ErrorWriteError on S3 failure.
    """
    # LOGIC — nothing to write if no rejections
    if rejected_df is None or rejected_df.empty:
        logger.info("No rejected rows for '%s'; skipping error file.", source_file)
        return None

    # LOGIC — construct S3 key
    error_key = f"{config.S3_ERRORS_PREFIX}{desk_code}_{trade_date}_errors.csv"

    # LOGIC — serialize to CSV bytes
    csv_bytes = rejected_df.to_csv(index=False).encode("utf-8")

    logger.info(
        "Writing %d rejected rows to s3://%s/%s",
        len(rejected_df),
        config.S3_BUCKET,
        error_key,
    )

    # LOGIC — upload to S3
    try:
        s3_client = boto3.client("s3")
        s3_client.put_object(Bucket=config.S3_BUCKET, Key=error_key, Body=csv_bytes)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise ErrorWriteError(
            f"S3 ClientError [{error_code}] writing error file '{error_key}'."
        ) from exc
    except Exception as exc:
        raise ErrorWriteError(
            f"Unexpected error writing error file '{error_key}': {type(exc).__name__}"
        ) from exc

    logger.info("Error file written to s3://%s/%s", config.S3_BUCKET, error_key)
    return error_key