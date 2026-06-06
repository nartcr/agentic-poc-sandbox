# BOILERPLATE
import io
import logging
import re
from typing import Tuple, List

import pandas as pd
import boto3

logger = logging.getLogger(__name__)

# LOGIC — compiled once at module level; matches {desk_code}_{trade_date}_positions.csv
_FILENAME_RE = re.compile(r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$")


def list_pending_files(s3_client, bucket: str, prefix: str) -> List[str]:
    # LOGIC — enumerate all objects under prefix, filter by naming convention
    matching_keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            # LOGIC — extract the filename component (last path segment)
            filename = key.split("/")[-1]
            if _FILENAME_RE.match(filename):
                matching_keys.append(key)
                logger.info("Discovered pending file: s3://%s/%s", bucket, key)
            else:
                logger.debug(
                    "Skipping key (does not match naming pattern): %s", key
                )

    logger.info(
        "list_pending_files found %d matching file(s) under s3://%s/%s",
        len(matching_keys),
        bucket,
        prefix,
    )
    return matching_keys


def read_csv_from_s3(
    s3_client, bucket: str, key: str
) -> Tuple[pd.DataFrame, str, str]:
    # LOGIC — extract desk_code and trade_date from the S3 key filename
    filename = key.split("/")[-1]
    match = _FILENAME_RE.match(filename)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected filename pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'"
        )

    desk_code: str = match.group(1)
    trade_date: str = match.group(2)

    logger.info(
        "Reading s3://%s/%s (desk_code=%s, trade_date=%s)",
        bucket,
        key,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — download S3 object body into memory
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()

    # LOGIC — read CSV with all columns as strings, no type inference
    df: pd.DataFrame = pd.read_csv(
        io.BytesIO(raw_bytes),
        dtype=str,
        keep_default_na=False,  # prevent pandas from converting "" or "NA" to NaN
    )

    # LOGIC — strip leading/trailing whitespace from all string values
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    logger.info(
        "Read %d row(s) and %d column(s) from s3://%s/%s",
        len(df),
        len(df.columns),
        bucket,
        key,
    )

    return df, desk_code, trade_date