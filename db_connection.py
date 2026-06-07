# BOILERPLATE
import logging

import psycopg2
import psycopg2.extensions

from secret_client import get_db_credentials

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_connection(secret_id: str) -> psycopg2.extensions.connection:
    # LOGIC
    """
    Create and return a psycopg2 connection to Aurora PostgreSQL.

    Credentials are retrieved from Secrets Manager via secret_client.
    connect_timeout is set to 10 seconds as specified in the design.
    Called once per Lambda invocation and passed down to db_loader and audit_logger.
    """
    credentials = get_db_credentials(secret_id)

    host = credentials["host"]
    port = int(credentials["port"])
    dbname = credentials["dbname"]
    user = credentials["username"]
    password = credentials["password"]

    logger.info(
        "Opening psycopg2 connection to host=%s port=%d dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )

    # LOGIC — connect_timeout=10 per design spec; no persistent pool (Lambda execution model)
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )

    logger.info("psycopg2 connection established successfully")
    return conn


def close_connection(conn: psycopg2.extensions.connection) -> None:
    # LOGIC
    """
    Safely close the psycopg2 connection if it is open.

    Guards against None and already-closed connections.
    """
    if conn is None:
        logger.debug("close_connection called with None — nothing to close")
        return

    try:
        if conn.closed == 0:
            conn.close()
            logger.info("psycopg2 connection closed")
        else:
            logger.debug("close_connection called on already-closed connection — no-op")
    except Exception as exc:  # LOGIC — never let a close failure propagate and mask the real error
        logger.warning("Exception while closing psycopg2 connection: %s", exc)