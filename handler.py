import logging
import os
import json
import boto3
from datetime import datetime

import pytz

from transform import process_records, load_secret

# BOILERPLATE
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

ET = pytz.timezone("America/Toronto")


def _now_et() -> str:
    # BOILERPLATE
    return datetime.now(ET).isoformat()


def handler(event: dict, context=None) -> dict:
    # LOGIC
    logger.info("Handler invoked at %s", _now_et())

    secret_name = os.environ.get("SECRET_NAME")
    if not secret_name:
        logger.error("SECRET_NAME environment variable is not set")
        raise EnvironmentError("SECRET_NAME environment variable is required")

    secret = load_secret(secret_name)
    logger.info("Secret loaded successfully for name=%s", secret_name)

    records = event.get("records", [])
    if not isinstance(records, list):
        logger.error("Expected 'records' to be a list, got %s", type(records).__name__)
        raise TypeError("'records' must be a list")

    logger.info("Processing %d record(s)", len(records))
    result = process_records(records, secret)
    logger.info("Processing complete at %s — %d record(s) produced", _now_et(), len(result))

    return {"processed": result, "count": len(result)}