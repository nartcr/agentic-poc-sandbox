# BOILERPLATE
import json
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# BOILERPLATE — module-level logger; all output via logging, never print()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — in-process cache for Lambda warm reuse; keyed by secret_id
_cache: dict[str, dict] = {}

# BOILERPLATE — single boto3 client instance reused across calls
_sm_client = boto3.client("secretsmanager")


def get_secret(secret_id: str) -> dict:
    """
    Retrieve and cache a Secrets Manager secret by secret_id.

    Returns the parsed JSON dict from SecretString.
    Raises RuntimeError if the secret cannot be retrieved or parsed.
    Never logs secret values.
    """
    # LOGIC — return cached value on warm Lambda reuse
    if secret_id in _cache:
        logger.info("Returning cached secret for secret_id=%s", secret_id)
        return _cache[secret_id]

    # LOGIC — fetch secret from Secrets Manager
    logger.info("Fetching secret from Secrets Manager: secret_id=%s", secret_id)
    try:
        response = _sm_client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: "
            f"ClientError code={error_code}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    # LOGIC — extract SecretString from response
    secret_string = response.get("SecretString")
    if secret_string is None:
        raise RuntimeError(
            f"Secret '{secret_id}' does not contain a SecretString. "
            "Binary secrets are not supported."
        )

    # LOGIC — parse JSON; raise clearly on malformed secret
    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate that the parsed value is a dict (not a bare string or list)
    if not isinstance(secret_dict, dict):
        raise RuntimeError(
            f"Secret '{secret_id}' parsed to type {type(secret_dict).__name__}, "
            "expected a JSON object (dict)."
        )

    # LOGIC — store in cache; never log the dict contents
    _cache[secret_id] = secret_dict
    logger.info("Secret fetched and cached successfully: secret_id=%s", secret_id)
    return secret_dict