import json
import logging
import os

import boto3
import psycopg2
import psycopg2.extensions

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class CredentialFetchError(Exception):
    # LOGIC — typed exception for any failure in credential retrieval or DB connection
    pass


def _fetch_secret_json() -> dict:
    # LOGIC — retrieve and parse the DB credentials secret from Secrets Manager
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Fetching DB credentials from Secrets Manager. SecretId=%s", secret_id)

    try:
        sm_client = boto3.client("secretsmanager")
        response = sm_client.get_secret_value(SecretId=secret_id)
    except Exception as exc:
        logger.error(
            "Failed to retrieve secret from Secrets Manager. SecretId=%s error=%s",
            secret_id,
            str(exc),
        )
        raise CredentialFetchError(
            f"Secrets Manager retrieval failed for SecretId={secret_id}: {exc}"
        ) from exc

    secret_string = response.get("SecretString")
    if not secret_string:
        logger.error(
            "Secrets Manager response contained no SecretString. SecretId=%s",
            secret_id,
        )
        raise CredentialFetchError(
            f"Secrets Manager returned empty SecretString for SecretId={secret_id}"
        )

    try:
        credentials = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        logger.error(
            "Failed to parse secret JSON. SecretId=%s error=%s",
            secret_id,
            str(exc),
        )
        raise CredentialFetchError(
            f"Secret JSON parse failed for SecretId={secret_id}: {exc}"
        ) from exc

    return credentials


def get_db_connection() -> psycopg2.extensions.connection:
    # LOGIC — fetch credentials and return an open psycopg2 DB connection
    credentials = _fetch_secret_json()

    required_keys = {"username", "password", "host", "port", "dbname"}
    missing_keys = required_keys - set(credentials.keys())
    if missing_keys:
        raise CredentialFetchError(
            f"Secret is missing required keys: {sorted(missing_keys)}"
        )

    host = credentials["host"]
    port = int(credentials["port"])
    dbname = credentials["dbname"]
    username = credentials["username"]

    logger.info(
        "Opening DB connection. host=%s port=%d dbname=%s username=%s",
        host,
        port,
        dbname,
        username,
    )

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=username,
            password=credentials["password"],
            connect_timeout=10,
        )
    except psycopg2.Error as exc:
        logger.error(
            "psycopg2 connection failed. host=%s port=%d dbname=%s error=%s",
            host,
            port,
            dbname,
            str(exc),
        )
        raise CredentialFetchError(
            f"Database connection failed for host={host} dbname={dbname}: {exc}"
        ) from exc

    logger.info("DB connection established successfully.")
    return conn