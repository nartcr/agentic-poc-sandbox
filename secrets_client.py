# BOILERPLATE
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — module-level cache; None until first successful retrieval
_cached_credentials: dict | None = None


def get_db_credentials() -> dict:
    # LOGIC — return cached credentials if already retrieved in this execution environment
    global _cached_credentials
    if _cached_credentials is not None:
        logger.debug("Returning cached DB credentials.")
        return _cached_credentials

    # BOILERPLATE — read secret identifier from environment; never hardcode
    secret_id = os.environ["DB_SECRET_ID"]
    logger.debug("Retrieving DB credentials from Secrets Manager. secret_id=%s", secret_id)

    # BOILERPLATE — boto3 client; credentials supplied by Lambda execution role
    client = boto3.client("secretsmanager")

    # LOGIC — call Secrets Manager; allow ClientError to propagate naturally (satisfies TAC-8)
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError:
        logger.error(
            "Failed to retrieve secret from Secrets Manager. secret_id=%s",
            secret_id,
        )
        raise

    # LOGIC — parse SecretString as JSON; extract required keys
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — validate that all expected keys are present before caching
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing_keys = required_keys - credentials.keys()
    if missing_keys:
        raise ValueError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing_keys)}"
        )

    # LOGIC — cache the result for subsequent calls within this Lambda execution environment
    _cached_credentials = credentials
    logger.debug(
        "DB credentials retrieved and cached. host=%s dbname=%s",
        credentials.get("host"),
        credentials.get("dbname"),
    )

    return _cached_credentials