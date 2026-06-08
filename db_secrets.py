# BOILERPLATE
import json
import logging
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — module-level cache; None until first successful retrieval
_cached_credentials: dict | None = None

# LOGIC — required keys that must be present in the secret JSON
_REQUIRED_KEYS = ("host", "port", "dbname", "username", "password")


def get_db_credentials() -> dict:
    """
    Retrieve database credentials from AWS Secrets Manager.

    Reads the secret ID from the DB_SECRET_ID environment variable.
    Caches the result in a module-level variable after the first call so
    subsequent invocations within the same Lambda execution context do not
    incur an additional Secrets Manager round-trip.

    Returns:
        dict with keys: host, port, dbname, username, password

    Raises:
        RuntimeError: if the secret cannot be retrieved or is malformed.
    """
    # LOGIC — return cached credentials on warm Lambda container reuse
    global _cached_credentials
    if _cached_credentials is not None:
        logger.debug("Returning cached database credentials.")
        return _cached_credentials

    # BOILERPLATE — read secret identifier from environment
    secret_id = os.environ["DB_SECRET_ID"]

    # BOILERPLATE — create Secrets Manager client inside function for testability
    client = boto3.client("secretsmanager")

    # LOGIC — fetch the secret value; surface a safe error on any failure
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Failed to retrieve database credentials from Secrets Manager "
            f"(error code: {error_code}). Check IAM permissions and secret name."
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(
            "A low-level AWS error occurred while retrieving database credentials."
        ) from exc

    # LOGIC — parse the secret JSON string
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            "Secrets Manager response did not contain a SecretString. "
            "Binary secrets are not supported."
        )

    try:
        credentials = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Database credentials secret is not valid JSON. "
            "Verify the secret format in Secrets Manager."
        ) from exc

    # LOGIC — validate that all required keys are present before caching
    missing = [k for k in _REQUIRED_KEYS if k not in credentials]
    if missing:
        raise RuntimeError(
            f"Database credentials secret is missing required keys: {missing}. "
            "Verify the secret structure in Secrets Manager."
        )

    # LOGIC — cache and return
    _cached_credentials = credentials
    logger.info("Database credentials retrieved and cached successfully.")
    return _cached_credentials