# BOILERPLATE
import io
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


def _get_client():
    # BOILERPLATE — obtain a boto3 S3 client; no caching to module-level variable
    # so that unit tests can patch boto3.client cleanly
    return boto3.client("s3")


def list_objects(bucket: str, prefix: str) -> list:
    # LOGIC — list all object keys under the given prefix using pagination
    # to handle buckets with more than 1000 objects
    client = _get_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    try:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
    except (BotoCoreError, ClientError) as exc:
        logger.error(
            "Failed to list objects in bucket=%s prefix=%s: %s",
            bucket,
            prefix,
            exc,
        )
        raise
    logger.info(
        "Listed %d objects from bucket=%s prefix=%s", len(keys), bucket, prefix
    )
    return keys


def download_fileobj(bucket: str, key: str) -> io.BytesIO:
    # LOGIC — download S3 object body into an in-memory BytesIO buffer;
    # seek to position 0 before returning so callers can read from the start
    client = _get_client()
    buffer = io.BytesIO()
    try:
        client.download_fileobj(bucket, key, buffer)
    except (BotoCoreError, ClientError) as exc:
        logger.error(
            "Failed to download s3://%s/%s: %s", bucket, key, exc
        )
        raise
    buffer.seek(0)
    logger.info("Downloaded s3://%s/%s (%d bytes)", bucket, key, buffer.getbuffer().nbytes)
    return buffer


def upload_bytes(bucket: str, key: str, data: bytes, content_type: str) -> None:
    # LOGIC — upload raw bytes to S3 with the specified ContentType;
    # wraps bytes in BytesIO so boto3 upload_fileobj can stream it
    client = _get_client()
    buffer = io.BytesIO(data)
    try:
        client.upload_fileobj(
            buffer,
            bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error(
            "Failed to upload to s3://%s/%s: %s", bucket, key, exc
        )
        raise
    logger.info(
        "Uploaded %d bytes to s3://%s/%s (content_type=%s)",
        len(data),
        bucket,
        key,
        content_type,
    )