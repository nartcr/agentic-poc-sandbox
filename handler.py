# BOILERPLATE
import json
import logging
import re
import urllib.parse

import pipeline
from config import Config

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — pattern guard: only process keys ending in _positions.csv
_POSITIONS_KEY_RE = re.compile(r".*_positions\.csv$")


def lambda_handler(event: dict, context: object) -> dict:  # LOGIC
    """
    Lambda entry point. Parses S3 PUT event notifications, validates each
    object key against the _positions.csv pattern, and dispatches to
    pipeline.run_pipeline for each matching record.

    Returns {"statusCode": 200, "processed": [keys]} on full success.
    Returns {"statusCode": 500, "error": str(e)} if any record raises.
    """
    # BOILERPLATE
    logger.info("lambda_handler invoked. Records count: %d", len(event.get("Records", [])))

    records = event.get("Records", [])
    if not records:
        logger.warning("Event contained no Records; nothing to process.")
        return {"statusCode": 200, "processed": []}

    # LOGIC — load config once to obtain the expected bucket name for validation
    try:
        cfg = Config()
    except EnvironmentError as cfg_err:
        logger.error("Configuration error: %s", cfg_err)
        return {"statusCode": 500, "error": str(cfg_err)}

    processed_keys = []
    last_error: Exception | None = None

    for record in records:  # LOGIC — iterate each S3 event record
        try:
            # LOGIC — extract bucket and key from S3 event notification structure
            s3_info = record["s3"]
            event_bucket = s3_info["bucket"]["name"]
            raw_key = s3_info["object"]["key"]

            # LOGIC — S3 event notifications URL-encode the object key
            s3_key = urllib.parse.unquote_plus(raw_key)

            logger.info("Processing S3 record: bucket=%s key=%s", event_bucket, s3_key)

            # LOGIC — guard: bucket in event must match configured S3_BUCKET
            if event_bucket != cfg.S3_BUCKET:
                logger.warning(
                    "Skipping key %s: event bucket %s does not match configured bucket %s",
                    s3_key,
                    event_bucket,
                    cfg.S3_BUCKET,
                )
                continue

            # LOGIC — guard: only process keys matching *_positions.csv pattern
            if not _POSITIONS_KEY_RE.match(s3_key):
                logger.warning(
                    "Skipping key %s: does not match required pattern *_positions.csv",
                    s3_key,
                )
                continue

            # LOGIC — dispatch to pipeline for full end-to-end processing
            pipeline.run_pipeline(s3_key)

            processed_keys.append(s3_key)
            logger.info("Successfully processed key: %s", s3_key)

        except Exception as exc:  # LOGIC — capture per-record failures
            logger.error(
                "Failed to process record. Key: %s. Error: %s: %s",
                record.get("s3", {}).get("object", {}).get("key", "<unknown>"),
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            last_error = exc
            # LOGIC — continue processing remaining records; collect first error
            # to surface in response

    # LOGIC — return 500 if any record failed, preserving last error message
    if last_error is not None:
        return {
            "statusCode": 500,
            "error": str(last_error),
            "processed": processed_keys,
        }

    return {"statusCode": 200, "processed": processed_keys}