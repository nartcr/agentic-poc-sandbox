# BOILERPLATE
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE


@dataclass
class AuditRecord:  # LOGIC
    s3_key: str
    desk_code: str
    trade_date: str
    processing_timestamp_et: datetime
    outcome: str                      # "SUCCESS" | "FAILURE"
    total_rows_received: int
    rows_loaded: int
    rows_rejected: int
    rows_skipped_duplicate: int
    error_detail: Optional[str]       # None on success
    service_identity: str             # Lambda function name or "local"


# LOGIC
_INSERT_AUDIT_SQL = """
INSERT INTO demo_schema.pipeline_audit
    (s3_key,
     desk_code,
     trade_date,
     processing_timestamp_et,
     outcome,
     total_rows_received,
     rows_loaded,
     rows_rejected,
     rows_skipped_duplicate,
     error_detail,
     service_identity)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def write_audit_record(conn, audit_record: AuditRecord) -> None:
    """
    # LOGIC
    Inserts one row into demo_schema.pipeline_audit.
    Commits independently of the main loader transaction so that
    audit evidence is always persisted regardless of pipeline outcome.
    """
    logger.info(
        "Writing audit record: s3_key=%s outcome=%s",
        audit_record.s3_key,
        audit_record.outcome,
    )

    # LOGIC — ensure timestamp is timezone-aware ET before persisting
    ts = audit_record.processing_timestamp_et
    if ts.tzinfo is None:
        ts = ET.localize(ts)
    else:
        ts = ts.astimezone(ET)

    params = (
        audit_record.s3_key,
        audit_record.desk_code,
        audit_record.trade_date,
        ts,
        audit_record.outcome,
        audit_record.total_rows_received,
        audit_record.rows_loaded,
        audit_record.rows_rejected,
        audit_record.rows_skipped_duplicate,
        audit_record.error_detail,
        audit_record.service_identity,
    )

    cursor = conn.cursor()  # LOGIC
    try:
        cursor.execute(_INSERT_AUDIT_SQL, params)
        conn.commit()  # LOGIC — independent commit from loader transaction
        logger.info("Audit record committed for s3_key=%s", audit_record.s3_key)
    except Exception:
        conn.rollback()
        logger.exception(
            "Failed to write audit record for s3_key=%s", audit_record.s3_key
        )
        raise
    finally:
        cursor.close()  # BOILERPLATE