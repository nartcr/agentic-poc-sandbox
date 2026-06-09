# BOILERPLATE
import json
import logging
import os

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache; None until first successful retrieval within this Lambda invocation
_CACHED_CREDENTIALS: dict | None = None


def get_db_credentials() -> dict:
    # LOGIC — return cached credentials if already retrieved in this invocation
    global _CACHED_CREDENTIALS
    if _CACHED_CREDENTIALS is not None:
        logger.info("Returning cached database credentials.")
        return _CACHED_CREDENTIALS

    # BOILERPLATE — read secret ID from environment; no hardcoded values
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Retrieving database credentials from Secrets Manager. secret_id=%s", secret_id)

    # BOILERPLATE — boto3 client; region is resolved from the Lambda execution environment
    client = boto3.client("secretsmanager")

    # LOGIC — fetch and parse secret value
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]
    raw: dict = json.loads(secret_string)

    # LOGIC — cast port to int; Secrets Manager stores all JSON values as strings
    credentials = {
        "host": str(raw["host"]),
        "port": int(raw["port"]),
        "dbname": str(raw["dbname"]),
        "username": str(raw["username"]),
        "password": str(raw["password"]),
    }

    # LOGIC — populate module-level cache for subsequent calls within this invocation
    _CACHED_CREDENTIALS = credentials
    logger.info(
        "Database credentials retrieved and cached. host=%s dbname=%s",
        credentials["host"],
        credentials["dbname"],
    )

    return _CACHED_CREDENTIALS