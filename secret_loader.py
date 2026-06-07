# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — module-level cache; populated at most once per Lambda invocation
_cached_credentials: dict | None = None


def get_db_credentials(secret_id: str) -> dict:
    # LOGIC — retrieve and cache credentials from Secrets Manager
    global _cached_credentials

    if _cached_credentials is not None:
        logger.info("Returning cached database credentials (secret_id=%s)", secret_id)
        return _cached_credentials

    logger.info("Fetching database credentials from Secrets Manager (secret_id=%s)", secret_id)

    # BOILERPLATE
    client = boto3.client("secretsmanager")

    # LOGIC
    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — validate required keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - set(credentials.keys())
    if missing:
        raise KeyError(
            f"Secrets Manager secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    _cached_credentials = credentials
    logger.info("Database credentials successfully loaded and cached (secret_id=%s)", secret_id)
    return _cached_credentials


def get_db_connection() -> psycopg2.extensions.connection:
    # LOGIC — resolve secret ID from environment; fetch credentials; open connection
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Opening database connection using secret_id=%s", secret_id)

    credentials = get_db_credentials(secret_id)

    # LOGIC — connect to the application database; port may be stored as str or int
    conn = psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname="app",
        user=credentials["username"],
        password=credentials["password"],
    )

    logger.info(
        "Database connection established (host=%s dbname=app)",
        credentials["host"],
    )
    return conn