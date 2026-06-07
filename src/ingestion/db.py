# BOILERPLATE
import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2

from src.ingestion.secrets import get_db_credentials

logger = logging.getLogger(__name__)


# LOGIC
@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that yields a psycopg2 connection using credentials
    retrieved at runtime from Secrets Manager.

    SSL is enforced via sslmode="require".
    Credentials are sourced exclusively from secrets.get_db_credentials().
    Connection is closed in a finally block to guarantee cleanup.
    """
    # LOGIC
    creds = get_db_credentials()

    logger.debug(
        "Opening database connection to host=%s dbname=%s port=%d",
        creds["host"],
        creds["dbname"],
        creds["port"],
    )

    # BOILERPLATE
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        user=creds["username"],
        password=creds["password"],
        dbname=creds["dbname"],
        sslmode="require",
    )

    # LOGIC
    try:
        logger.debug("Database connection established successfully.")
        yield conn
    except Exception:
        logger.exception("Exception occurred during database operation; rolling back.")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.debug("Database connection closed.")