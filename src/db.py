# BOILERPLATE
import logging
from contextlib import contextmanager

import psycopg2

from src import secrets  # BOILERPLATE: runtime credential retrieval

logger = logging.getLogger(__name__)  # BOILERPLATE


@contextmanager
def get_connection():
    """
    Context manager that opens a psycopg2 connection to Aurora PostgreSQL.
    Credentials are fetched fresh from Secrets Manager on every call.
    Closes the connection on exit regardless of exception.

    Usage:
        with db.get_connection() as conn:
            ...
    """
    # LOGIC: fetch credentials at runtime — never cached, never hardcoded
    creds = secrets.get_db_credentials()

    host = creds["host"]
    port = int(creds["port"])
    dbname = creds["dbname"]
    user = creds["username"]
    password = creds["password"]

    logger.info(
        "Opening database connection: host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )

    conn = None
    try:
        # LOGIC: establish psycopg2 connection using credentials from Secrets Manager
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        yield conn
    finally:
        # LOGIC: always close the connection, even if an exception occurred
        if conn is not None:
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as close_exc:
                logger.warning(
                    "Exception while closing database connection: %s", close_exc
                )