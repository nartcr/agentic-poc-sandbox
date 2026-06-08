# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2

from pipeline_exceptions import CredentialError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — required keys that must be present in the Secrets Manager JSON payload
_REQUIRED_SECRET_KEYS = {"host", "port", "dbname", "username", "password"}


def _fetch_secret(secret_id: str) -> dict:
    # LOGIC — retrieve and parse the Secrets Manager secret; raise CredentialError on any failure
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        logger.error("Failed to retrieve secret '%s' from Secrets Manager: %s", secret_id, exc)
        raise CredentialError(f"Could not retrieve secret '{secret_id}': {exc}") from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        logger.error("Secret '%s' returned an empty or binary SecretString", secret_id)
        raise CredentialError(f"Secret '{secret_id}' has no SecretString value")

    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        logger.error("Secret '%s' SecretString is not valid JSON: %s", secret_id, exc)
        raise CredentialError(f"Secret '{secret_id}' SecretString could not be parsed as JSON: {exc}") from exc

    # LOGIC — validate all required keys are present before attempting to connect
    missing = _REQUIRED_SECRET_KEYS - set(secret_dict.keys())
    if missing:
        logger.error("Secret '%s' is missing required keys: %s", secret_id, missing)
        raise CredentialError(f"Secret '{secret_id}' is missing required keys: {missing}")

    return secret_dict


def get_connection() -> psycopg2.extensions.connection:
    # LOGIC — read secret ID from environment; never hardcode credentials
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Fetching database credentials from Secrets Manager secret: %s", secret_id)

    secret = _fetch_secret(secret_id)

    host = secret["host"]
    port = int(secret["port"])
    dbname = secret["dbname"]
    user = secret["username"]
    password = secret["password"]

    logger.info("Connecting to database '%s' at %s:%d as user '%s'", dbname, host, port, user)

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode="require",
        )
        logger.info("Database connection established successfully")
        return conn
    except psycopg2.OperationalError as exc:
        logger.error("psycopg2 connection failed for host=%s dbname=%s: %s", host, dbname, exc)
        raise CredentialError(f"Database connection failed: {exc}") from exc