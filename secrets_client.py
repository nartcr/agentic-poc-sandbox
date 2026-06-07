# BOILERPLATE
import json
import logging
import os

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache; persists across warm Lambda invocations within the same container
_secret_cache: dict = {}


def get_secret(secret_id: str) -> dict:
    """
    # LOGIC
    Retrieves a secret from AWS Secrets Manager by secret_id.
    Parses the JSON string from SecretString and returns the credential dict.
    Caches the result in _secret_cache keyed by secret_id to avoid redundant
    API calls within a single Lambda invocation (and across warm invocations).

    Args:
        secret_id: The Secrets Manager secret identifier (e.g. "agentic-poc-aurora").

    Returns:
        dict with keys: host, port, username, password, dbname (and any others
        stored in the secret).

    Raises:
        botocore.exceptions.ClientError: if the secret does not exist or access is denied.
        json.JSONDecodeError: if SecretString is not valid JSON.
        KeyError: if the secret is stored as a binary secret (SecretBinary) rather than
                  SecretString.
    """
    # LOGIC — return cached value if available
    if secret_id in _secret_cache:
        logger.debug("Returning cached secret for secret_id='%s'", secret_id)
        return _secret_cache[secret_id]

    # BOILERPLATE — create Secrets Manager client
    client = boto3.client("secretsmanager")

    logger.info("Fetching secret from Secrets Manager: secret_id='%s'", secret_id)

    # LOGIC — retrieve the secret value from AWS
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — parse SecretString as JSON
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — populate cache before returning
    _secret_cache[secret_id] = credentials

    logger.info(
        "Secret retrieved and cached: secret_id='%s' keys=%s",
        secret_id,
        list(credentials.keys()),
    )

    return credentials