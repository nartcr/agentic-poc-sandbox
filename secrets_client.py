# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache so Secrets Manager is called at most once per cold start
_CACHED_CREDENTIALS: dict | None = None


def get_db_credentials() -> dict:
    # LOGIC
    """
    Retrieve Aurora PostgreSQL credentials from AWS Secrets Manager.

    Reads the secret identified by os.environ["DB_SECRET_ID"] and parses
    the JSON payload. Result is cached at module level so Secrets Manager
    is only called once per Lambda cold start.

    Returns:
        dict with keys: host, port, dbname, username, password
    """
    global _CACHED_CREDENTIALS

    if _CACHED_CREDENTIALS is not None:
        logger.debug("Returning cached DB credentials (Secrets Manager not re-called).")
        return _CACHED_CREDENTIALS

    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Fetching DB credentials from Secrets Manager. secret_id=%s", secret_id)

    # BOILERPLATE
    client = boto3.client("secretsmanager")

    # LOGIC
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]
    raw: dict = json.loads(secret_string)

    # LOGIC — validate required keys are present before caching
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - raw.keys()
    if missing:
        raise KeyError(
            f"Secrets Manager secret '{secret_id}' is missing required keys: {missing}"
        )

    _CACHED_CREDENTIALS = {
        "host": str(raw["host"]),
        "port": int(raw["port"]),
        "dbname": str(raw["dbname"]),
        "username": str(raw["username"]),
        "password": str(raw["password"]),
    }

    logger.info(
        "DB credentials loaded and cached. host=%s dbname=%s",
        _CACHED_CREDENTIALS["host"],
        _CACHED_CREDENTIALS["dbname"],
    )
    return _CACHED_CREDENTIALS