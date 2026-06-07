import logging  # BOILERPLATE
import psycopg2  # BOILERPLATE
from datetime import datetime  # BOILERPLATE
from typing import Optional  # BOILERPLATE

logger = logging.getLogger(__name__)  # BOILERPLATE


def record_audit(
    conn,
    desk_code: str,
    trade_date: str,
    source_s3_key: str,
    status: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: Optional[str],
    processing_timestamp: datetime,
    service_identity: str = "unknown/unknown",
) -> None:
    # LOGIC — inserts one audit row per processing attempt into demo_schema.pipeline_audit
    sql = """
        INSERT INTO demo_schema.pipeline_audit (
            desk_code,
            trade_date,
            source_s3_key,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
            error_message,
            processing_timestamp,
            service_identity
        )
        VALUES (
            %(desk_code)s,
            %(trade_date)s::date,
            %(source_s3_key)s,
            %(status)s,
            %(total_rows)s,
            %(rows_inserted)s,
            %(rows_rejected)s,
            %(error_message)s,
            %(processing_timestamp)s,
            %(service_identity)s
        )
    """
    params = {
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_s3_key": source_s3_key,
        "status": status,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "processing_timestamp": processing_timestamp,
        "service_identity": service_identity,
    }
    # LOGIC — trade_date is stored as YYYYMMDD string in S3 filenames; cast to DATE in SQL
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        logger.info(
            "Audit record written: desk_code=%s trade_date=%s status=%s "
            "total_rows=%d rows_inserted=%d rows_rejected=%d",
            desk_code,
            trade_date,
            status,
            total_rows,
            rows_inserted,
            rows_rejected,
        )
    except psycopg2.Error as exc:
        # LOGIC — log but re-raise; caller decides whether this is fatal
        logger.error(
            "Failed to write audit record for desk_code=%s trade_date=%s: %s",
            desk_code,
            trade_date,
            exc,
        )
        raise