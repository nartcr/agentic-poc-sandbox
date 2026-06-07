# BOILERPLATE
import json
import logging
import re
import urllib.parse
from datetime import datetime

import pytz

from config import Config
import pipeline

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact regex from design specification
_KEY_PATTERN = re.compile(r"^incoming/[^/]+_\d{8}_positions\.csv$")

_ET = pytz.timezone("America/Toronto")


def lambda_handler(event: dict, context) -> dict:
    # BOILERPLATE — Lambda entry point for S3 ObjectCreated events
    now_et = datetime.now(_ET)
    logger.info(
        "lambda_handler invoked at %s — records received: %d",
        now_et.isoformat(),
        len(event.get("Records", [])),
    )

    # LOGIC — load config once per invocation (reads env vars)
    try:
        cfg = Config()
    except Exception as exc:
        logger.error("Failed to load Config: %s", exc)
        return {"statusCode": 500, "error": "Configuration error — see logs"}

    processed_keys: list[str] = []
    errors: list[str] = []

    records = event.get("Records", [])
    if not records:
        logger.warning("No Records in event payload; nothing to process")
        return {"statusCode": 200, "processed": []}

    for record in records:
        # LOGIC — extract bucket and key from S3 event record
        try:
            raw_key: str = record["s3"]["object"]["key"]
            # S3 keys in events are URL-encoded
            s3_key: str = urllib.parse.unquote_plus(raw_key)
            bucket: str = record["s3"]["bucket"]["name"]
        except (KeyError, TypeError) as exc:
            logger.error("Malformed S3 event record — skipping: %s", exc)
            errors.append(f"malformed_record: {exc}")
            continue

        # LOGIC — filter: only process keys matching the expected pattern
        if not _KEY_PATTERN.match(s3_key):
            logger.info(
                "Skipping key '%s' — does not match expected pattern '%s'",
                s3_key,
                _KEY_PATTERN.pattern,
            )
            continue

        logger.info("Processing S3 key: s3://%s/%s", bucket, s3_key)

        try:
            report = pipeline.process_file(s3_key=s3_key, config=cfg)
            processed_keys.append(s3_key)
            logger.info(
                "Successfully processed '%s' — inserted: %d, rejected: %d",
                s3_key,
                report.get("rows_inserted", 0),
                report.get("rows_rejected", 0),
            )
        except Exception as exc:  # LOGIC — log failure but continue remaining records
            logger.error("Failed to process '%s': %s", s3_key, exc, exc_info=True)
            errors.append(f"{s3_key}: {exc}")

    # LOGIC — determine response status code
    if errors and not processed_keys:
        # All qualifying keys failed
        return {"statusCode": 500, "error": "; ".join(errors)}

    return {"statusCode": 200, "processed": processed_keys}