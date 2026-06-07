# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache; None on cold start, populated after first retrieval
_CACHED_CREDENTIALS = None

_REQUIRED_KEYS = ("host", "port", "dbname", "username", "password")


def get_db_credentials() -> dict:
    # LOGIC — return cached credentials on warm Lambda invocation
    global _CACHED_CREDENTIALS
    if _CACHED_CREDENTIALS is not None:
        logger.debug("Returning cached DB credentials.")
        return _CACHED_CREDENTIALS

    # BOILERPLATE — retrieve secret from Secrets Manager at runtime
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Fetching DB credentials from Secrets Manager. SecretId: %s", secret_id)

    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — parse the secret JSON string
    secret_string = response.get("SecretString")
    if secret_string is None:
        raise RuntimeError(
            "Secrets Manager response did not contain 'SecretString'. "
            "Binary secrets are not supported."
        )

    try:
        credentials = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse Secrets Manager secret as JSON: {exc}"
        ) from exc

    # LOGIC — validate all required keys are present
    missing = [key for key in _REQUIRED_KEYS if key not in credentials]
    if missing:
        raise RuntimeError(
            f"DB credentials secret is missing required keys: {missing}. "
            f"Expected keys: {list(_REQUIRED_KEYS)}"
        )

    logger.info("DB credentials retrieved and cached successfully.")
    _CACHED_CREDENTIALS = credentials
    return _CACHED_CREDENTIALS