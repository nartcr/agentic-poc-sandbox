# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2
import psycopg2.extensions

from ingestion_exceptions import DBConnectionError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _fetch_secret(secret_id: str) -> dict:
    # LOGIC — retrieve and parse the JSON secret from Secrets Manager at runtime
    logger.info("Fetching database credentials from Secrets Manager: secret_id=%s", secret_id)
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as e:
        logger.error(
            "Failed to retrieve secret from Secrets Manager: secret_id=%s error=%s",
            secret_id,
            str(e),
        )
        raise DBConnectionError(
            f"Unable to retrieve database secret '{secret_id}' from Secrets Manager"
        ) from e

    # LOGIC — the secret value is a JSON string; parse it into a dict
    try:
        secret_string = response["SecretString"]
        credentials = json.loads(secret_string)
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(
            "Failed to parse secret JSON: secret_id=%s error=%s",
            secret_id,
            str(e),
        )
        raise DBConnectionError(
            f"Database secret '{secret_id}' is not valid JSON or is missing 'SecretString'"
        ) from e

    # LOGIC — validate that all required keys are present in the secret
    required_keys = {"username", "password", "host", "port"}
    missing = required_keys - set(credentials.keys())
    if missing:
        logger.error(
            "Secret is missing required keys: secret_id=%s missing=%s",
            secret_id,
            missing,
        )
        raise DBConnectionError(
            f"Database secret '{secret_id}' is missing required keys: {missing}"
        )

    logger.info(
        "Database credentials successfully retrieved: secret_id=%s host=%s",
        secret_id,
        credentials.get("host"),
    )
    return credentials


def get_connection() -> psycopg2.extensions.connection:
    # LOGIC — read secret ID and DB name from environment variables (never hardcoded)
    secret_id = os.environ["DB_SECRET_ID"]
    db_name = os.environ["DB_NAME"]

    # LOGIC — fetch credentials at runtime from Secrets Manager
    credentials = _fetch_secret(secret_id)

    host = credentials["host"]
    port = int(credentials["port"])  # LOGIC — defensively cast; may arrive as str or int
    username = credentials["username"]
    password = credentials["password"]

    # LOGIC — open a psycopg2 connection using retrieved credentials; raise DBConnectionError on failure
    logger.info(
        "Opening database connection: host=%s port=%d dbname=%s user=%s",
        host,
        port,
        db_name,
        username,
    )
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db_name,
            user=username,
            password=password,
        )
    except psycopg2.Error as e:
        logger.error(
            "psycopg2 connection failed: host=%s port=%d dbname=%s user=%s error=%s",
            host,
            port,
            db_name,
            username,
            str(e),
        )
        raise DBConnectionError(
            f"Failed to connect to database at host='{host}' dbname='{db_name}'"
        ) from e

    logger.info(
        "Database connection established successfully: host=%s dbname=%s",
        host,
        db_name,
    )
    return conn