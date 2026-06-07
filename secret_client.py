# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache so repeated calls within the same Lambda invocation
# do not make redundant Secrets Manager API calls (TAC-8)
_CREDENTIALS_CACHE: dict[str, dict] = {}


def get_db_credentials(secret_id: str) -> dict:
    # LOGIC
    """
    Retrieve database credentials from AWS Secrets Manager.

    Returns a dict with keys: username, password, host, port, dbname.
    Results are cached at module level for the lifetime of the Lambda container.
    """
    if secret_id in _CREDENTIALS_CACHE:
        logger.info("Returning cached credentials for secret_id=%s", secret_id)
        return _CREDENTIALS_CACHE[secret_id]

    logger.info("Fetching credentials from Secrets Manager for secret_id=%s", secret_id)

    # BOILERPLATE
    client = boto3.client("secretsmanager")

    # LOGIC
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — validate that all expected keys are present before caching
    required_keys = {"username", "password", "host", "port", "dbname"}
    missing_keys = required_keys - credentials.keys()
    if missing_keys:
        raise KeyError(
            f"Secrets Manager secret '{secret_id}' is missing required keys: {missing_keys}"
        )

    _CREDENTIALS_CACHE[secret_id] = credentials
    logger.info("Credentials cached for secret_id=%s", secret_id)
    return credentials