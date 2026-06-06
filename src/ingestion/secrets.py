# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache; populated on first call within a Lambda execution context
_cached_credentials = None


def get_db_credentials() -> dict:
    # LOGIC
    global _cached_credentials

    if _cached_credentials is not None:
        logger.debug("Returning cached DB credentials.")
        return _cached_credentials

    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Retrieving DB credentials from Secrets Manager. SecretId=%s", secret_id)

    # BOILERPLATE
    client = boto3.client("secretsmanager")

    # LOGIC
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — validate expected keys are present before caching
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - credentials.keys()
    if missing:
        raise KeyError(
            "DB credentials secret is missing required keys: %s" % ", ".join(sorted(missing))
        )

    _cached_credentials = credentials
    logger.info("DB credentials retrieved and cached successfully.")
    return _cached_credentials