# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

import secret_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")

# LOGIC — exact SQL for inserting one audit record using column names from the data contract
_INSERT_AUDIT_SQL = """
    INSERT INTO demo_schema.pipeline_audit
        (filename, desk_code, trade_date, status, total_rows, rows_inserted, rows_rejected,
         error_message, processing_timestamp_et)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: str | None,
) -> None:
    # LOGIC — resolve current ET timestamp for audit record
    processing_timestamp_et = datetime.now(_ET)

    # BOILERPLATE — retrieve credentials at runtime; never hardcoded
    creds = secret_manager.get_db_credentials()

    logger.info(
        "Writing audit record: filename=%s desk_code=%s trade_date=%s status=%s "
        "total_rows=%d rows_inserted=%d rows_rejected=%d",
        filename,
        desk_code,
        trade_date,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    # BOILERPLATE — open psycopg2 connection using Secrets Manager credentials
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )

    try:
        with conn:
            with conn.cursor() as cur:
                # LOGIC — insert one row per file processed; trade_date may be None if key parse failed
                cur.execute(
                    _INSERT_AUDIT_SQL,
                    (
                        filename,
                        desk_code,
                        trade_date,   # psycopg2 passes None as SQL NULL; string YYYY-MM-DD cast to DATE
                        status,
                        total_rows,
                        rows_inserted,
                        rows_rejected,
                        error_message,
                        processing_timestamp_et,
                    ),
                )
        logger.info("Audit record written successfully for filename=%s status=%s", filename, status)
    finally:
        # BOILERPLATE — always close connection
        conn.close()