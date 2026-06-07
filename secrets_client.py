# BOILERPLATE
import json
import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)

# LOGIC — module-level cache populated once per Lambda container lifetime
_CREDENTIALS_CACHE: Dict[str, object] = {}

_REQUIRED_KEYS = ("host", "port", "dbname", "username", "password")


def get_db_credentials(sm_client) -> dict:
    # LOGIC — return cached credentials on warm invocation
    if _CREDENTIALS_CACHE:
        logger.debug("Returning cached DB credentials from module-level cache.")
        return dict(_CREDENTIALS_CACHE)

    # BOILERPLATE — read secret ID from environment
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Fetching DB credentials from Secrets Manager. secret_id=%s", secret_id)

    # LOGIC — fetch secret from Secrets Manager
    try:
        response = sm_client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    # LOGIC — parse the secret JSON payload
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' does not contain a SecretString value."
        )

    try:
        raw_creds = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate all required keys are present and non-empty
    missing_keys = [k for k in _REQUIRED_KEYS if k not in raw_creds or raw_creds[k] == ""]
    if missing_keys:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {missing_keys}"
        )

    # LOGIC — coerce port to int; Secrets Manager may store it as a string
    try:
        port_value = int(raw_creds["port"])
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' field 'port' cannot be coerced to int: {raw_creds['port']!r}"
        ) from exc

    credentials = {
        "host": str(raw_creds["host"]),
        "port": port_value,
        "dbname": str(raw_creds["dbname"]),
        "username": str(raw_creds["username"]),
        "password": str(raw_creds["password"]),
    }

    # LOGIC — populate module-level cache for warm invocation reuse
    _CREDENTIALS_CACHE.update(credentials)
    logger.info(
        "DB credentials successfully fetched and cached. host=%s dbname=%s",
        credentials["host"],
        credentials["dbname"],
    )

    return dict(credentials)