# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache keyed by secret_id; avoids repeated Secrets Manager
# API calls within a single Lambda invocation (warm or cold start after first call)
_CACHE: dict[str, dict] = {}


def _get_secretsmanager_client():
    # BOILERPLATE
    return boto3.client("secretsmanager")


def get_db_credentials(secret_id: str) -> dict:
    # LOGIC — return cached credentials if available; otherwise fetch from
    # Secrets Manager and populate the cache before returning
    if secret_id in _CACHE:
        logger.debug("Returning cached credentials for secret_id=%s", secret_id)
        return _CACHE[secret_id]

    logger.info("Fetching database credentials from Secrets Manager for secret_id=%s", secret_id)

    try:
        client = _get_secretsmanager_client()
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        # LOGIC — never log the secret value or detailed credential info
        logger.error(
            "Failed to retrieve secret from Secrets Manager for secret_id=%s. "
            "Check IAM permissions and that the secret exists.",
            secret_id,
        )
        raise RuntimeError(
            f"Could not retrieve database credentials from Secrets Manager "
            f"(secret_id={secret_id}). Check Lambda execution role permissions."
        ) from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        logger.error(
            "Secrets Manager returned an empty SecretString for secret_id=%s",
            secret_id,
        )
        raise RuntimeError(
            f"Secrets Manager secret is empty or binary-only (secret_id={secret_id})."
        )

    try:
        credentials = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        logger.error(
            "Secrets Manager secret is not valid JSON for secret_id=%s",
            secret_id,
        )
        raise RuntimeError(
            f"Secrets Manager secret could not be parsed as JSON (secret_id={secret_id})."
        ) from exc

    # LOGIC — validate that all required keys are present before caching
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing_keys = required_keys - set(credentials.keys())
    if missing_keys:
        logger.error(
            "Secrets Manager secret for secret_id=%s is missing required keys: %s",
            secret_id,
            sorted(missing_keys),
        )
        raise RuntimeError(
            f"Secrets Manager secret is missing required credential fields "
            f"(secret_id={secret_id}). Expected keys: {sorted(required_keys)}."
        )

    # LOGIC — cache and return; do not log credential values
    _CACHE[secret_id] = credentials
    logger.info(
        "Database credentials successfully retrieved and cached for secret_id=%s. "
        "host=%s port=%s dbname=%s",
        secret_id,
        credentials["host"],
        credentials["port"],
        credentials["dbname"],
    )

    return credentials