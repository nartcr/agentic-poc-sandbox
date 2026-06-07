# BOILERPLATE
import json
import logging
import os

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache: populated on first call, reused within the same Lambda execution environment
_SECRET_CACHE: dict = {}
_CACHE_KEY = "__db_secret__"


def get_db_secret() -> dict:
    # LOGIC — return cached secret if already loaded in this execution environment
    if _CACHE_KEY in _SECRET_CACHE:
        logger.debug("Returning cached DB secret")
        return _SECRET_CACHE[_CACHE_KEY]

    # LOGIC — read secret ID from environment variable; no hardcoded values
    secret_id: str = os.environ["DB_SECRET_ID"]
    logger.info("Fetching DB secret from Secrets Manager: secret_id=%s", secret_id)

    # BOILERPLATE — create Secrets Manager client at call time (not module load) to respect Lambda cold-start patterns
    client = boto3.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — secret is stored as a JSON string; parse and validate required keys
    secret_string: str = response["SecretString"]
    secret_dict: dict = json.loads(secret_string)

    # LOGIC — validate that all required keys are present before caching
    required_keys = {"host", "port", "username", "password", "dbname"}
    missing = required_keys - secret_dict.keys()
    if missing:
        raise KeyError(
            f"DB secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    logger.info(
        "DB secret loaded successfully: host=%s dbname=%s",
        secret_dict["host"],
        secret_dict["dbname"],
    )

    # LOGIC — cache the parsed dict so subsequent calls within the same invocation skip Secrets Manager
    _SECRET_CACHE[_CACHE_KEY] = secret_dict
    return secret_dict