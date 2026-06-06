# BOILERPLATE
import logging
from datetime import datetime

import psycopg2
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — constant service name as specified in design
_SERVICE_NAME = "trade-position-ingestion"

# LOGIC — exact table and column names from data contract
_INSERT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (source_key, desk_code, trade_date, outcome,
     total_rows, loaded_rows, rejected_rows,
     error_detail, processed_at, service_name)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(
    credentials: dict,
    source_key: str,
    desk_code: str,
    trade_date: str,
    outcome: str,
    total_rows: int,
    loaded_rows: int,
    rejected_rows: int,
    error_detail: str | None,
    processed_at: datetime,
) -> None:
    # LOGIC — validate outcome value against allowed set
    allowed_outcomes = {"SUCCESS", "PARTIAL", "FAILURE"}
    if outcome not in allowed_outcomes:
        raise ValueError(
            f"Invalid outcome '{outcome}'. Must be one of {allowed_outcomes}."
        )

    # LOGIC — ensure processed_at is ET-aware; if naive, localise to ET
    et_tz = pytz.timezone("America/Toronto")
    if processed_at.tzinfo is None:
        processed_at = et_tz.localize(processed_at)

    # BOILERPLATE — build connection from credentials dict
    conn = None
    try:
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            user=credentials["username"],
            password=credentials["password"],
            dbname=credentials["dbname"],
        )

        with conn:
            with conn.cursor() as cur:
                # LOGIC — insert one audit row; parameters match column order in SQL
                cur.execute(
                    _INSERT_SQL,
                    (
                        source_key,
                        desk_code,
                        trade_date,
                        outcome,
                        total_rows,
                        loaded_rows,
                        rejected_rows,
                        error_detail,
                        processed_at,
                        _SERVICE_NAME,
                    ),
                )

        logger.info(
            "Audit record written: source_key=%s desk_code=%s trade_date=%s outcome=%s",
            source_key,
            desk_code,
            trade_date,
            outcome,
        )

    except psycopg2.Error as exc:
        # LOGIC — log without exposing credential values
        logger.error(
            "Failed to write audit record for source_key=%s: %s",
            source_key,
            exc,
        )
        raise RuntimeError(
            f"Audit write failed for source_key={source_key}: {exc}"
        ) from exc

    finally:
        # BOILERPLATE — always close connection
        if conn is not None:
            conn.close()