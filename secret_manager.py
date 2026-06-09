# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level singleton; reused across warm Lambda invocations
_db_connection = None


class SecretRetrievalError(Exception):
    """Raised when AWS Secrets Manager cannot be reached or returns an error."""


def _parse_secret(secret_string: str) -> dict:
    # LOGIC
    """
    Parse the raw secret JSON string into a credentials dict.

    Expected keys: host, port, dbname, username, password.
    Raises ValueError if any required key is absent.
    """
    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise SecretRetrievalError(
            f"Secret value is not valid JSON: {exc}"
        ) from exc

    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - set(secret_dict.keys())
    if missing:
        raise SecretRetrievalError(
            f"Secret JSON is missing required keys: {sorted(missing)}"
        )

    return secret_dict


def get_db_connection() -> psycopg2.extensions.connection:
    # LOGIC
    """
    Return a live psycopg2 connection to Aurora PostgreSQL.

    Reads DB_SECRET_ID from the environment, fetches credentials from
    AWS Secrets Manager at runtime, and builds a psycopg2 connection.
    Caches the connection at module level for reuse across warm Lambda
    invocations. Verifies the cached connection is still open before
    returning it; reconnects if it has been closed.

    Raises:
        SecretRetrievalError: if Secrets Manager cannot be reached or
            the secret value cannot be parsed.
        psycopg2.Error: if the database connection cannot be established.
    """
    global _db_connection  # BOILERPLATE — module-level singleton pattern

    # LOGIC — return cached connection if still alive
    if _db_connection is not None and _db_connection.closed == 0:
        logger.info("Reusing existing database connection from module-level cache.")
        return _db_connection

    # BOILERPLATE — read secret ID from environment; never hardcode
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Retrieving database credentials from Secrets Manager for secret: %s", secret_id)

    # BOILERPLATE — build boto3 client against the existing Secrets Manager service
    sm_client = boto3.client("secretsmanager")

    try:
        response = sm_client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        logger.error(
            "Failed to retrieve secret '%s' from Secrets Manager: %s",
            secret_id,
            exc,
        )
        raise SecretRetrievalError(
            f"Unable to retrieve secret '{secret_id}': {exc}"
        ) from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        raise SecretRetrievalError(
            f"Secret '{secret_id}' returned no SecretString value."
        )

    # LOGIC — parse the secret JSON and extract individual credentials
    credentials = _parse_secret(secret_string)

    host = credentials["host"]
    port = int(credentials["port"])
    dbname = credentials["dbname"]
    user = credentials["username"]
    password = credentials["password"]

    logger.info(
        "Establishing psycopg2 connection to host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )

    # LOGIC — open psycopg2 connection; exceptions propagate to caller
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )

    logger.info("Database connection established successfully.")

    # LOGIC — cache connection for reuse within this Lambda execution context
    _db_connection = conn
    return _db_connection