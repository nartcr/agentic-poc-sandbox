# BOILERPLATE
import logging
import os
from datetime import datetime

import psycopg2
import pytz

from db_secrets import get_db_credentials

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
def write_audit_record(
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
) -> None:
    """
    Writes one row to demo_schema.pipeline_audit capturing the full
    processing outcome for regulatory traceability.

    status must be one of: 'SUCCESS', 'PARTIAL', 'FAILED'.
    desk_code and trade_date may be None when a FAILED audit is written
    before those values were parsed.
    """
    # LOGIC — validate status value
    allowed_statuses = {"SUCCESS", "PARTIAL", "FAILED"}
    if status not in allowed_statuses:
        raise ValueError(
            f"Invalid audit status '{status}'. Must be one of: {allowed_statuses}"
        )

    # LOGIC — capture processing timestamp in Eastern Time
    et_zone = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_zone)

    logger.info(
        "Writing audit record: filename=%s desk_code=%s trade_date=%s "
        "status=%s total_rows=%d rows_inserted=%d rows_rejected=%d",
        filename,
        desk_code,
        trade_date,
        status,
        total_rows,
        rows_inserted,
        rows_rejected,
    )

    # BOILERPLATE — retrieve credentials and open a dedicated connection
    credentials = get_db_credentials()

    conn = psycopg2.connect(
        host=credentials["host"],
        port=credentials["port"],
        dbname=credentials["dbname"],
        user=credentials["username"],
        password=credentials["password"],
    )

    try:
        with conn:
            with conn.cursor() as cur:
                # LOGIC — INSERT using exact column names from data contract
                cur.execute(
                    """
                    INSERT INTO demo_schema.pipeline_audit
                        (filename,
                         desk_code,
                         trade_date,
                         status,
                         total_rows,
                         rows_inserted,
                         rows_rejected,
                         error_message,
                         processing_timestamp_et,
                         created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    """,
                    (
                        filename,
                        desk_code,
                        trade_date,
                        status,
                        total_rows,
                        rows_inserted,
                        rows_rejected,
                        error_message,
                        processing_timestamp_et,
                    ),
                )
        logger.info(
            "Audit record committed for filename=%s status=%s", filename, status
        )
    except Exception as exc:
        logger.error(
            "Failed to write audit record for filename=%s: %s", filename, exc
        )
        raise
    finally:
        conn.close()