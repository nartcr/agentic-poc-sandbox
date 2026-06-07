# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache persists across calls within a single Lambda execution context
# but resets on cold start (module is re-imported on each cold start)
_CREDENTIALS_CACHE: dict | None = None


def _get_secretsmanager_client():
    # BOILERPLATE
    return boto3.client("secretsmanager")


def get_db_credentials() -> dict:
    """
    Retrieve Aurora PostgreSQL credentials from AWS Secrets Manager.
    Returns a dict with keys: host, port, dbname, username, password.
    Caches the result within the Lambda execution context to avoid redundant API calls.
    """
    # LOGIC — return cached credentials if already retrieved in this execution context
    global _CREDENTIALS_CACHE
    if _CREDENTIALS_CACHE is not None:
        logger.debug("Returning cached DB credentials.")
        return _CREDENTIALS_CACHE

    # BOILERPLATE — read secret ID from environment; never hardcode credentials
    secret_id = os.environ["DB_SECRET_ID"]

    logger.info("Retrieving DB credentials from Secrets Manager. SecretId=%s", secret_id)

    # LOGIC — fetch the secret value from Secrets Manager at runtime
    client = _get_secretsmanager_client()
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — the secret is stored as a JSON string; parse it into a dict
    secret_string = response["SecretString"]
    secret_dict = json.loads(secret_string)

    # LOGIC — validate that all required keys are present before caching
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing_keys = required_keys - set(secret_dict.keys())
    if missing_keys:
        raise KeyError(
            f"DB credentials secret is missing required keys: {sorted(missing_keys)}"
        )

    # LOGIC — extract only the required credential fields (ignore any extra keys in secret)
    _CREDENTIALS_CACHE = {
        "host": secret_dict["host"],
        "port": secret_dict["port"],
        "dbname": secret_dict["dbname"],
        "username": secret_dict["username"],
        "password": secret_dict["password"],
    }

    logger.info("DB credentials successfully retrieved and cached.")

    return _CREDENTIALS_CACHE