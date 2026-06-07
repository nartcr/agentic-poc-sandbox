# BOILERPLATE
import logging

import psycopg2
import psycopg2.extensions

import secrets_client

logger = logging.getLogger(__name__)


def get_connection(sm_client) -> psycopg2.extensions.connection:
    # LOGIC — retrieve credentials via secrets_client (uses module-level cache on warm invocations)
    credentials = secrets_client.get_db_credentials(sm_client)

    host = credentials["host"]
    port = credentials["port"]
    dbname = credentials["dbname"]
    user = credentials["username"]   # LOGIC — psycopg2 uses 'user', not 'username'
    password = credentials["password"]

    logger.info(
        "Opening Aurora PostgreSQL connection. host=%s port=%s dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )

    # LOGIC — create connection with explicit connect_timeout per design spec
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10,
        )
    except psycopg2.OperationalError as exc:
        raise RuntimeError(
            f"Failed to connect to Aurora PostgreSQL at {host}:{port}/{dbname}: {exc}"
        ) from exc

    # LOGIC — caller controls transaction boundaries; set autocommit=False explicitly
    conn.autocommit = False
    logger.info("Aurora PostgreSQL connection established successfully.")

    return conn