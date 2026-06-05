import logging
import json
import boto3
from datetime import datetime

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")


def _now_et() -> str:
    # BOILERPLATE
    return datetime.now(ET).isoformat()


def load_secret(secret_name: str) -> dict:
    # BOILERPLATE — reads secret from existing Secrets Manager; never creates resources
    client = boto3.client("secretsmanager")
    logger.info("Fetching secret: %s at %s", secret_name, _now_et())
    response = client.get_secret_value(SecretId=secret_name)
    secret_string = response.get("SecretString", "{}")
    return json.loads(secret_string)


def validate_record(record: dict) -> bool:
    # LOGIC
    if not isinstance(record, dict):
        logger.warning("Record is not a dict: %s", record)
        return False
    if "value" not in record:
        logger.warning("Record missing 'value' key: %s", record)
        return False
    return True


def transform_record(record: dict, secret: dict) -> dict:
    # LOGIC — enriches a single record; idempotent: re-running produces the same shape
    prefix = secret.get("transform_prefix", "processed")
    raw_value = record["value"]

    transformed = {
        "original_value": raw_value,
        "transformed_value": f"{prefix}:{raw_value}",
        "processed_at": _now_et(),
    }

    # Carry forward any extra keys from the original record (except 'value')
    for key, val in record.items():
        if key != "value":
            transformed.setdefault(key, val)

    return transformed


def process_records(records: list, secret: dict) -> list:
    # LOGIC — filters invalid records, transforms valid ones; idempotent per record
    results = []
    for idx, record in enumerate(records):
        if not validate_record(record):
            logger.warning("Skipping invalid record at index %d", idx)
            continue
        transformed = transform_record(record, secret)
        results.append(transformed)
        logger.info("Transformed record %d: %s -> %s", idx, record["value"], transformed["transformed_value"])
    return results