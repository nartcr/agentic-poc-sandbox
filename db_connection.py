# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def _fetch_secret(secret_id: str) -> dict:
    """Retrieve and parse the database credentials JSON from Secrets Manager."""
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: {exc}"
        ) from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' exists but contains no SecretString value."
        )

    try:
        secret_dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate all required keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - set(secret_dict.keys())
    if missing:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    return secret_dict


# LOGIC
def get_connection():
    """
    Fetch DB credentials from Secrets Manager and return an open psycopg2 connection.
    Caller is responsible for closing the connection.
    Raises RuntimeError if the secret cannot be fetched or the connection fails.
    """
    # LOGIC — read secret ID from environment; no hardcoded values
    secret_id = os.environ.get("DB_SECRET_ID")
    if not secret_id:
        raise RuntimeError(
            "Environment variable 'DB_SECRET_ID' is not set or is empty."
        )

    logger.info("Fetching database credentials from Secrets Manager for secret: %s", secret_id)
    secret = _fetch_secret(secret_id)

    host = secret["host"]
    port = int(secret["port"])
    dbname = secret["dbname"]
    username = secret["username"]
    password = secret["password"]

    logger.info(
        "Opening psycopg2 connection to host=%s port=%d dbname=%s user=%s",
        host, port, dbname, username,
    )

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=password,
            connect_timeout=10,
        )
    except psycopg2.OperationalError as exc:
        raise RuntimeError(
            f"psycopg2 failed to connect to database '{dbname}' at '{host}:{port}': {exc}"
        ) from exc

    logger.info("psycopg2 connection established successfully.")
    return conn