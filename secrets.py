import json
import logging
import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache keyed by secret_id to avoid redundant API calls
# within the same Lambda invocation
_cache: dict = {}

_REQUIRED_KEYS = {"host", "port", "dbname", "username", "password"}


def get_db_credentials(secret_id: str) -> dict:
    # LOGIC — return cached credentials if already retrieved this invocation
    if secret_id in _cache:
        logger.info("Returning cached DB credentials for secret_id=%s", secret_id)
        return _cache[secret_id]

    logger.info("Retrieving DB credentials from Secrets Manager. secret_id=%s", secret_id)

    # BOILERPLATE — create Secrets Manager client at call time (no hardcoded credentials)
    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        logger.error(
            "Failed to retrieve secret from Secrets Manager. secret_id=%s error=%s",
            secret_id,
            str(exc),
        )
        raise RuntimeError(
            f"Unable to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    # LOGIC — parse the secret string as JSON
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' exists but SecretString is empty or missing."
        )

    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate all required keys are present
    missing_keys = _REQUIRED_KEYS - set(secret_dict.keys())
    if missing_keys:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing_keys)}"
        )

    # LOGIC — cast port to int per data contract return type
    try:
        credentials = {
            "host": str(secret_dict["host"]),
            "port": int(secret_dict["port"]),
            "dbname": str(secret_dict["dbname"]),
            "username": str(secret_dict["username"]),
            "password": str(secret_dict["password"]),
        }
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' contains invalid value types: {exc}"
        ) from exc

    # LOGIC — store in module-level cache before returning
    _cache[secret_id] = credentials
    logger.info(
        "DB credentials retrieved and cached. secret_id=%s host=%s dbname=%s",
        secret_id,
        credentials["host"],
        credentials["dbname"],
    )

    return credentials