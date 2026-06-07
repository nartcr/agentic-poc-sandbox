# BOILERPLATE
import logging
import os

import psycopg2

import secrets_client

# BOILERPLATE
logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    # LOGIC
    # Retrieve credentials from Secrets Manager via the shared secrets_client module.
    # The secret ID is read from the environment variable DB_SECRET_ID at runtime.
    secret_id = os.environ["DB_SECRET_ID"]
    credentials = secrets_client.get_secret(secret_id)

    # LOGIC
    # Extract individual connection parameters from the secret JSON.
    # Key names are defined in the data contract:
    #   host, port, username, password, dbname
    host = credentials["host"]
    port = int(credentials["port"])
    username = credentials["username"]
    password = credentials["password"]
    dbname = credentials["dbname"]

    # LOGIC
    # Open a new psycopg2 connection to the Aurora PostgreSQL instance.
    # Connection is not pooled — caller is responsible for closing it.
    # All SQL executed through this connection targets demo_schema.
    logger.info(
        "Opening database connection: host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        username,
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        dbname=dbname,
    )

    logger.info("Database connection established successfully.")
    return conn