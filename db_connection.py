# BOILERPLATE
import json
import logging
import os

import boto3
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def get_connection() -> psycopg2.extensions.connection:
    """
    Retrieve database credentials from Secrets Manager at runtime and
    return a live psycopg2 connection.  Caller is responsible for closing
    the connection.  No credentials are stored at module scope or logged.
    """
    # LOGIC — fetch secret from Secrets Manager; never cache at module level
    secret_id = os.environ["DB_SECRET_ID"]
    logger.info("Retrieving database credentials from Secrets Manager secret: %s", secret_id)

    # BOILERPLATE — Secrets Manager client
    secrets_client = boto3.client("secretsmanager")

    secret_response = secrets_client.get_secret_value(SecretId=secret_id)
    secret_payload = json.loads(secret_response["SecretString"])

    # LOGIC — extract all required connection parameters from the secret JSON
    db_host = secret_payload["host"]
    db_port = int(secret_payload["port"])
    db_name = secret_payload["dbname"]
    db_user = secret_payload["username"]
    db_password = secret_payload["password"]

    logger.info(
        "Opening psycopg2 connection to host=%s port=%s dbname=%s user=%s",
        db_host,
        db_port,
        db_name,
        db_user,
    )

    # LOGIC — open connection; sslmode required by design contract
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
        sslmode="require",
    )

    logger.info("psycopg2 connection opened successfully")
    return conn