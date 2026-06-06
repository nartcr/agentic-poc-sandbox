# BOILERPLATE
import logging
import re

import boto3
from botocore.exceptions import ClientError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — filename pattern as specified in the approved design
_FILENAME_PATTERN = re.compile(
    r'^incoming/([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$'
)


def read_position_file(bucket: str, key: str) -> tuple:
    """
    Downloads object at s3://{bucket}/{key}.
    Parses filename to extract desk_code and trade_date from key
    using regex: r'^incoming/([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$'
    Returns (file_bytes, {"desk_code": str, "trade_date": str, "s3_key": str}).
    Raises ValueError if filename does not match expected pattern.
    Raises FileNotFoundError if object does not exist.
    """
    # LOGIC — validate filename pattern before any network call
    match = _FILENAME_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            r"'incoming/{{DESK_CODE}}_{{YYYY-MM-DD}}_positions.csv'"
        )

    desk_code = match.group(1)
    trade_date = match.group(2)

    logger.info(
        "Attempting to read S3 object: bucket=%s key=%s desk_code=%s trade_date=%s",
        bucket, key, desk_code, trade_date
    )

    # BOILERPLATE — S3 client uses Lambda execution role credentials
    s3_client = boto3.client("s3")

    # LOGIC — download object and check existence
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            raise FileNotFoundError(
                f"S3 object not found: s3://{bucket}/{key}"
            ) from exc
        logger.error(
            "Unexpected S3 ClientError reading s3://%s/%s: %s",
            bucket, key, exc
        )
        raise

    # LOGIC — read all bytes and validate non-empty
    file_bytes = response["Body"].read()
    if not file_bytes:
        raise ValueError(
            f"S3 object is empty: s3://{bucket}/{key}"
        )

    logger.info(
        "Successfully read %d bytes from s3://%s/%s",
        len(file_bytes), bucket, key
    )

    metadata = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "s3_key": key,
    }
    return file_bytes, metadata


def list_unprocessed_files(bucket: str, input_prefix: str) -> list:
    """
    Lists all S3 keys under {bucket}/{input_prefix} matching
    pattern *_positions.csv.
    Returns list of full S3 keys.
    """
    # BOILERPLATE
    s3_client = boto3.client("s3")

    # LOGIC — use paginator to handle > 1000 objects
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=input_prefix)

    matching_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # LOGIC — filter to keys matching the positions CSV pattern
            if key.endswith("_positions.csv"):
                matching_keys.append(key)
                logger.debug("Found unprocessed file: %s", key)

    logger.info(
        "Listed %d unprocessed file(s) under s3://%s/%s",
        len(matching_keys), bucket, input_prefix
    )
    return matching_keys