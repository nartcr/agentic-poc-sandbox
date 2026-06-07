# BOILERPLATE
import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extensions

from secrets_client import get_db_credentials

# BOILERPLATE
logger = logging.getLogger(__name__)


# LOGIC
@contextmanager
def get_connection(secret_id: str) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that opens a psycopg2 connection to Aurora PostgreSQL,
    commits on clean exit, rolls back on exception, and always closes the
    connection on exit.

    Usage:
        with get_connection(os.environ["DB_SECRET_ID"]) as conn:
            with conn.cursor() as cur:
                cur.execute(...)

    Args:
        secret_id: The Secrets Manager secret ID holding DB credentials.

    Yields:
        psycopg2.extensions.connection: An open database connection.

    Raises:
        RuntimeError: If credentials cannot be retrieved from Secrets Manager.
        psycopg2.OperationalError: If the database connection cannot be established.
    """
    # LOGIC — retrieve credentials from Secrets Manager; no credentials in code
    credentials = get_db_credentials(secret_id)

    host = credentials["host"]
    port = int(credentials["port"])  # LOGIC — defensive cast; secret may store port as str or int
    dbname = credentials["dbname"]
    username = credentials["username"]
    password = credentials["password"]

    logger.info(
        "Opening database connection to host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        username,
    )

    # LOGIC — establish connection
    conn: psycopg2.extensions.connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=username,
        password=password,
    )

    try:
        # LOGIC — yield connection to caller; caller performs DB operations here
        yield conn

        # LOGIC — clean exit: commit the transaction
        conn.commit()
        logger.info("Database transaction committed successfully.")

    except Exception:
        # LOGIC — exception during DB operations: roll back to leave DB in clean state
        conn.rollback()
        logger.exception("Database transaction rolled back due to exception.")
        raise

    finally:
        # BOILERPLATE — always close the connection regardless of outcome
        conn.close()
        logger.info("Database connection closed.")