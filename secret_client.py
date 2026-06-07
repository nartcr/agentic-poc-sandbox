# BOILERPLATE
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

from pipeline_exceptions import SecretFetchError

logger = logging.getLogger(__name__)

# BOILERPLATE — module-level cache; populated on first call, reused within Lambda container lifetime
_secret_cache: dict | None = None


def get_secret() -> dict:
    """Fetch database credentials from AWS Secrets Manager.

    Returns a dict with keys: host, port, dbname, username, password.
    Caches the result for the lifetime of the Lambda container so that
    repeated calls within one invocation do not incur extra Secrets Manager API calls.

    Raises SecretFetchError on any retrieval or parse failure.
    """
    # LOGIC — return cached value if already fetched this container lifetime
    global _secret_cache
    if _secret_cache is not None:
        logger.debug("Returning cached database secret.")
        return _secret_cache

    secret_id = os.environ["DB_SECRET_ID"]

    logger.info("Fetching database secret from Secrets Manager: %s", secret_id)

    # BOILERPLATE — build Secrets Manager client
    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "Failed to retrieve secret %s from Secrets Manager: %s — %s",
            secret_id,
            error_code,
            str(exc),
        )
        raise SecretFetchError(
            f"Could not retrieve secret '{secret_id}' from Secrets Manager: {error_code}"
        ) from exc

    # LOGIC — parse the secret string as JSON
    secret_string = response.get("SecretString")
    if not secret_string:
        logger.error(
            "Secret %s retrieved but SecretString is empty or missing.", secret_id
        )
        raise SecretFetchError(
            f"Secret '{secret_id}' has no SecretString value."
        )

    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        logger.error(
            "Secret %s is not valid JSON: %s", secret_id, str(exc)
        )
        raise SecretFetchError(
            f"Secret '{secret_id}' could not be parsed as JSON."
        ) from exc

    # LOGIC — validate that all required keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing_keys = required_keys - set(secret_dict.keys())
    if missing_keys:
        logger.error(
            "Secret %s is missing required keys: %s",
            secret_id,
            sorted(missing_keys),
        )
        raise SecretFetchError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing_keys)}"
        )

    # LOGIC — store in module-level cache
    _secret_cache = secret_dict
    logger.info("Database secret fetched and cached successfully.")

    return _secret_cache