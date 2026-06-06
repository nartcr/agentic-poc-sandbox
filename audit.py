# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
import pytz

from exceptions import SecretsError
from secrets import get_db_credentials

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — allowed outcome values per design contract
_VALID_OUTCOMES = frozenset({"SUCCESS", "PARTIAL", "FAILURE"})

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def record(
    source_file: str,
    outcome: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    processing_timestamp: datetime,
) -> None:
    """
    Write a single audit row to rfdh.ingestion_audit.

    This insert is intentionally NOT idempotent — each processing run,
    including retries, produces its own audit row (per design).

    Parameters
    ----------
    source_file            : S3 object key of the processed file.
    outcome                : One of 'SUCCESS', 'PARTIAL', 'FAILURE'.
    total_rows             : Total rows read from the input file.
    rows_loaded            : Rows actually inserted into rfdh.trade_positions.
    rows_rejected          : Rows rejected by validation.
    processing_timestamp   : ET-aware datetime representing when processing occurred.
    """
    # LOGIC — guard invalid outcome values before touching the database
    if outcome not in _VALID_OUTCOMES:
        raise ValueError(
            f"Invalid outcome '{outcome}'. Must be one of: {sorted(_VALID_OUTCOMES)}"
        )

    # LOGIC — ensure processing_timestamp is ET-aware; localize naive datetimes
    if processing_timestamp.tzinfo is None:
        logger.warning(
            "processing_timestamp has no tzinfo; localizing to ET as a precaution."
        )
        processing_timestamp = _ET.localize(processing_timestamp)

    # LOGIC — read service identity from environment (injected at deploy time, never hardcoded)
    service_identity = os.environ["SERVICE_IDENTITY"]

    # LOGIC — retrieve DB credentials from Secrets Manager (via cached helper)
    try:
        creds = get_db_credentials()
    except SecretsError as exc:
        logger.error("Cannot write audit record — credential retrieval failed: %s", exc)
        raise

    # BOILERPLATE — build connection parameters from secret payload
    conn_params = {
        "host": creds["host"],
        "port": int(creds["port"]),
        "dbname": creds["dbname"],
        "user": creds["username"],
        "password": creds["password"],
    }

    # LOGIC — SQL insert; deliberately no ON CONFLICT clause (non-idempotent by design)
    insert_sql = """
        INSERT INTO rfdh.ingestion_audit
            (source_file, outcome, total_rows, rows_loaded, rows_rejected,
             processing_timestamp, service_identity)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    conn = None
    try:
        conn = psycopg2.connect(**conn_params)  # BOILERPLATE
        with conn:  # BOILERPLATE — context manager commits on exit, rolls back on exception
            with conn.cursor() as cur:
                cur.execute(
                    insert_sql,
                    (
                        source_file,
                        outcome,
                        int(total_rows),
                        int(rows_loaded),
                        int(rows_rejected),
                        processing_timestamp,  # LOGIC — psycopg2 serialises tz-aware datetime as TIMESTAMPTZ
                        service_identity,
                    ),
                )
                logger.info(
                    "Audit record written: source_file=%s outcome=%s "
                    "total_rows=%d rows_loaded=%d rows_rejected=%d",
                    source_file,
                    outcome,
                    total_rows,
                    rows_loaded,
                    rows_rejected,
                )
    except psycopg2.Error as exc:
        logger.error(
            "Failed to write audit record for source_file='%s': %s",
            source_file,
            exc,
        )
        raise
    finally:
        # BOILERPLATE — always close connection; psycopg2 context manager handles
        # commit/rollback but does not close the connection itself
        if conn is not None:
            conn.close()