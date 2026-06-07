# BOILERPLATE
import logging
import datetime
import psycopg2

import secrets_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_INSERT_SQL = """
INSERT INTO demo_schema.pipeline_audit
  (filename, desk_code, trade_date, status, total_rows, rows_inserted, rows_rejected, error_message, processing_timestamp_et)
VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit(
    filename: str,
    desk_code,          # str | None — None when filename parse fails
    trade_date,         # datetime.date | None — None when filename parse fails
    status: str,        # "SUCCESS" or "FAILURE"
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,      # str | None
    processing_timestamp_et: datetime.datetime,
) -> None:
    # LOGIC — validate status value is one of the two accepted sentinels
    if status not in ("SUCCESS", "FAILURE"):
        raise ValueError(f"Invalid audit status '{status}': must be 'SUCCESS' or 'FAILURE'")

    # BOILERPLATE — fetch credentials and open connection
    conn = _get_connection()
    try:
        _insert_audit_row(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=error_message,
            processing_timestamp_et=processing_timestamp_et,
        )
        conn.commit()
        logger.info(
            "Audit record written: filename=%s status=%s total_rows=%d rows_inserted=%d rows_rejected=%d",
            filename,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except Exception:
        conn.rollback()
        logger.exception("Failed to write audit record for filename=%s", filename)
        raise
    finally:
        conn.close()


def _get_connection():
    # BOILERPLATE — retrieve DB credentials from Secrets Manager at runtime
    creds = secrets_client.get_db_credentials()
    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
    )
    return conn


def _insert_audit_row(
    conn,
    filename: str,
    desk_code,
    trade_date,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message,
    processing_timestamp_et: datetime.datetime,
) -> None:
    # LOGIC — execute the parameterised insert using the exact column order from the data contract
    with conn.cursor() as cur:
        cur.execute(
            _INSERT_SQL,
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
        logger.debug(
            "Executed pipeline_audit INSERT: rowcount=%d", cur.rowcount
        )