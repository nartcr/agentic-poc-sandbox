# BOILERPLATE
import json
import logging
import boto3

logger = logging.getLogger(__name__)

# LOGIC — module-level cache: one Secrets Manager call per secret_id per process lifetime
_cache: dict = {}

# LOGIC — the five keys required by the database connection contract
_REQUIRED_SECRET_KEYS = {"host", "port", "dbname", "username", "password"}


def get_db_credentials(secret_id: str) -> dict:
    # LOGIC — return cached credentials if already fetched for this secret_id
    if secret_id in _cache:
        logger.debug("Returning cached DB credentials for secret_id=%s", secret_id)
        return _cache[secret_id]

    logger.info("Fetching DB credentials from Secrets Manager: secret_id=%s", secret_id)

    # BOILERPLATE — boto3 Secrets Manager client; no credentials in code
    client = boto3.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — secret value is a JSON string; parse it
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — validate that all required keys are present before caching
    missing_keys = _REQUIRED_SECRET_KEYS - set(credentials.keys())
    if missing_keys:
        raise KeyError(
            "DB credentials secret is missing required keys: %s" % sorted(missing_keys)
        )

    # LOGIC — cache the credentials dict keyed by secret_id
    _cache[secret_id] = credentials

    logger.info(
        "DB credentials successfully retrieved and cached for secret_id=%s", secret_id
    )

    return credentials