# BOILERPLATE
import json
import logging

import boto3
from botocore.exceptions import ClientError

from pipeline_exceptions import SecretsRetrievalError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — module-level cache: keyed by secret_id, populated on first retrieval per Lambda invocation
_secret_cache: dict = {}


def get_secret(secret_id: str) -> dict:
    """Retrieve and cache a Secrets Manager secret by secret_id.

    Returns the parsed JSON dict from SecretString.
    Raises SecretsRetrievalError on any AWS ClientError.
    """
    # LOGIC — return cached value if already retrieved this invocation
    if secret_id in _secret_cache:
        logger.debug("Returning cached secret for secret_id=%s", secret_id)
        return _secret_cache[secret_id]

    # BOILERPLATE — create Secrets Manager client (no hardcoded credentials)
    client = boto3.client("secretsmanager")

    logger.info("Retrieving secret from Secrets Manager: secret_id=%s", secret_id)

    # LOGIC — call Secrets Manager and parse the JSON secret string
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        logger.error(
            "Failed to retrieve secret secret_id=%s: error_code=%s message=%s",
            secret_id,
            error_code,
            str(exc),
        )
        raise SecretsRetrievalError(
            f"Unable to retrieve secret '{secret_id}': [{error_code}] {exc}"
        ) from exc

    # LOGIC — parse SecretString as JSON and populate the cache
    raw_secret = response["SecretString"]
    try:
        secret_dict = json.loads(raw_secret)
    except json.JSONDecodeError as exc:
        logger.error(
            "Secret secret_id=%s is not valid JSON: %s", secret_id, str(exc)
        )
        raise SecretsRetrievalError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    _secret_cache[secret_id] = secret_dict
    logger.info("Secret retrieved and cached: secret_id=%s", secret_id)

    return secret_dict