# BOILERPLATE
import json
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from exceptions import SecretsError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — module-level cache; populated on first call within a Lambda invocation,
# reused on subsequent calls within the same invocation, cleared on cold start.
_cached_credentials: dict | None = None


def get_db_credentials() -> dict:
    # LOGIC
    global _cached_credentials

    if _cached_credentials is not None:
        logger.debug("Returning cached DB credentials (within-invocation cache hit).")
        return _cached_credentials

    secret_id = os.environ["DB_SECRET_ID"]  # LOGIC — read from env, never hardcoded
    logger.info("Fetching DB credentials from Secrets Manager. SecretId env var: DB_SECRET_ID")

    try:
        client = boto3.client("secretsmanager")  # BOILERPLATE
        response = client.get_secret_value(SecretId=secret_id)  # LOGIC
    except (BotoCoreError, ClientError) as exc:
        # LOGIC — wrap all Secrets Manager failures in SecretsError
        logger.error("Failed to retrieve secret from Secrets Manager: %s", exc)
        raise SecretsError(
            f"Unable to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    # LOGIC — parse JSON payload; raise SecretsError if malformed
    secret_string = response.get("SecretString")
    if secret_string is None:
        raise SecretsError(
            f"Secret '{secret_id}' has no SecretString field. Binary secrets are not supported."
        )

    try:
        credentials: dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise SecretsError(
            f"Secret '{secret_id}' is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate required keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - credentials.keys()
    if missing:
        raise SecretsError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    logger.info("DB credentials retrieved and cached for this invocation.")
    _cached_credentials = credentials
    return _cached_credentials