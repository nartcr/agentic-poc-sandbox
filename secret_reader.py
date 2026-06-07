# BOILERPLATE
import json
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

# LOGIC — the exact keys the pipeline expects in the Secrets Manager secret JSON
EXPECTED_SECRET_KEYS = {"host", "port", "username", "password", "dbname"}


def get_db_credentials(secret_id: str) -> dict[str, str]:
    """
    Retrieves database credentials from AWS Secrets Manager.

    Args:
        secret_id: The Secrets Manager secret identifier (e.g. "agentic-poc-aurora").

    Returns:
        A dict with keys: host, port, username, password, dbname.

    Raises:
        RuntimeError: If the secret cannot be retrieved, is not valid JSON,
                      or is missing any of the expected keys.
    """
    # BOILERPLATE — create a fresh client on each call; no caching to disk or module level
    sm_client = boto3.client("secretsmanager")

    logger.info("Retrieving database credentials from Secrets Manager. secret_id=%s", secret_id)

    # LOGIC — retrieve and parse the secret
    try:
        response = sm_client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager. "
            f"ErrorCode={error_code}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            f"BotoCore error retrieving secret '{secret_id}': {exc}"
        ) from exc

    # LOGIC — extract secret string from the response
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' returned an empty or binary SecretString. "
            "Only string secrets are supported."
        )

    # LOGIC — parse JSON payload
    try:
        credentials = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate all expected keys are present
    missing_keys = EXPECTED_SECRET_KEYS - set(credentials.keys())
    if missing_keys:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing_keys)}"
        )

    logger.info(
        "Database credentials retrieved successfully. secret_id=%s host=%s dbname=%s",
        secret_id,
        credentials.get("host", "<unknown>"),
        credentials.get("dbname", "<unknown>"),
    )

    # LOGIC — return only the expected keys as a clean typed dict; never log password
    return {
        "host": str(credentials["host"]),
        "port": str(credentials["port"]),
        "username": str(credentials["username"]),
        "password": str(credentials["password"]),
        "dbname": str(credentials["dbname"]),
    }