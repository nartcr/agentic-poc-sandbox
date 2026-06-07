# BOILERPLATE
import contextlib
import logging

import psycopg2

from secrets import DBCredentials

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@contextlib.contextmanager
def get_connection(credentials: DBCredentials):
    # LOGIC — open psycopg2 connection using runtime credentials; autocommit=False
    # so caller controls transaction boundaries explicitly (commit / rollback)
    conn = None
    try:
        conn = psycopg2.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.username,
            password=credentials.password,
            dbname=credentials.dbname,
        )
        conn.autocommit = False
        logger.info(
            "Database connection opened — host: %s, dbname: %s",
            credentials.host,
            credentials.dbname,
        )
        yield conn
    except psycopg2.OperationalError as exc:
        # LOGIC — sanitized error: do not log credential values
        logger.error(
            "Failed to open database connection to host '%s', dbname '%s': %s",
            credentials.host,
            credentials.dbname,
            exc,
        )
        raise
    finally:
        if conn is not None and not conn.closed:
            conn.close()
            logger.info("Database connection closed")