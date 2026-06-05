# BOILERPLATE
import logging

import psycopg2

logger = logging.getLogger(__name__)


def get_connection(credentials: dict):
    # LOGIC — open a psycopg2 connection using credentials supplied by secrets.py
    # sslmode=require and connect_timeout=10 are mandated by the design (NFR-3.2)
    logger.info(
        "Opening Aurora PostgreSQL connection: host=%s port=%s dbname=%s",
        credentials["host"],
        credentials["port"],
        credentials["dbname"],
    )
    conn = psycopg2.connect(
        host=credentials["host"],
        port=int(credentials["port"]),
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
        connect_timeout=10,
        sslmode="require",
    )
    logger.info("Aurora PostgreSQL connection established successfully")
    return conn