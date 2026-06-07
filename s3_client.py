# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

# BOILERPLATE — module-level S3 client; reused across invocations in the same Lambda container
_s3_client = None


def _get_client():
    # BOILERPLATE — lazy singleton for testability via dependency injection / mock patching
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def download_file(bucket: str, key: str) -> str:
    # LOGIC — download S3 object and return UTF-8 decoded string
    logger.info("Downloading s3://%s/%s", bucket, key)
    client = _get_client()
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    content = body.decode("utf-8")
    logger.info("Downloaded %d bytes from s3://%s/%s", len(body), bucket, key)
    return content


def upload_file(
    bucket: str,
    key: str,
    content: str,
    content_type: str = "text/csv",
) -> None:
    # LOGIC — upload string content to S3 at the given key
    logger.info("Uploading to s3://%s/%s (content_type=%s)", bucket, key, content_type)
    client = _get_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType=content_type,
    )
    logger.info("Upload complete: s3://%s/%s", bucket, key)


def write_manifest(bucket: str, manifest_key: str, manifest: dict) -> None:
    # LOGIC — serialize manifest dict to formatted JSON and upload to manifests/ prefix
    # Manifest key pattern: manifests/{desk_code}_{trade_date}_manifest.json
    # Always overwrites so it points to the latest run (idempotent by design)
    logger.info("Writing manifest to s3://%s/%s", bucket, manifest_key)

    if not manifest_key.startswith("manifests/"):
        # LOGIC — enforce path contract; do not silently write outside the manifests/ prefix
        raise ValueError(
            f"manifest_key must start with 'manifests/' — got: {manifest_key!r}"
        )

    manifest_json = json.dumps(manifest, indent=2)
    client = _get_client()
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_json.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("Manifest written: s3://%s/%s", bucket, manifest_key)