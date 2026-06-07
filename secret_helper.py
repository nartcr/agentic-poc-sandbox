# BOILERPLATE
import json
import logging

import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache avoids redundant Secrets Manager API calls
# within the same Lambda execution context
_secret_cache: dict[str, dict] = {}


def get_secret(secret_id: str) -> dict:
    # LOGIC — return cached secret if already fetched in this execution context
    if secret_id in _secret_cache:
        logger.debug("Returning cached secret for secret_id=%s", secret_id)
        return _secret_cache[secret_id]

    # BOILERPLATE — retrieve secret from AWS Secrets Manager
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        raise RuntimeError(
            f"Unable to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    # LOGIC — parse the secret string as JSON
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' exists but contains no SecretString value."
        )

    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate that all expected keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - set(secret_dict.keys())
    if missing:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    # LOGIC — store in module-level cache before returning
    _secret_cache[secret_id] = secret_dict
    logger.info("Secret successfully retrieved and cached for secret_id=%s", secret_id)
    return secret_dict