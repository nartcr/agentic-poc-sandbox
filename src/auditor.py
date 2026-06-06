# BOILERPLATE
import json
import logging
import datetime

import boto3
import psycopg2

logger = logging.getLogger(__name__)


def record_audit(credentials: dict, audit_record: dict) -> None:
    # LOGIC — fetches caller identity from STS and writes one audit row to app.pipeline_audit
    operator_identity = _get_operator_identity()

    conn = None
    try:
        # BOILERPLATE — open psycopg2 connection from credential dict
        conn = psycopg2.connect(
            host=credentials["host"],
            port=int(credentials["port"]),
            dbname=credentials["dbname"],
            user=credentials["username"],
            password=credentials["password"],
        )
        with conn.cursor() as cur:
            # LOGIC — insert audit row using named parameter syntax; never use positional %s here
            cur.execute(
                """
                INSERT INTO app.pipeline_audit
                  (source_file, pipeline_run_id, status, total_rows_received,
                   rows_loaded, rows_rejected, error_message, started_at,
                   completed_at, operator_identity)
                VALUES (%(source_file)s, %(pipeline_run_id)s, %(status)s,
                        %(total_rows_received)s, %(rows_loaded)s, %(rows_rejected)s,
                        %(error_message)s, %(started_at)s, %(completed_at)s,
                        %(operator_identity)s)
                """,
                {
                    "source_file": audit_record["source_file"],
                    "pipeline_run_id": audit_record["pipeline_run_id"],
                    "status": audit_record["status"],
                    "total_rows_received": audit_record.get("total_rows_received"),
                    "rows_loaded": audit_record.get("rows_loaded"),
                    "rows_rejected": audit_record.get("rows_rejected"),
                    "error_message": audit_record.get("error_message"),
                    "started_at": audit_record["started_at"],
                    "completed_at": audit_record["completed_at"],
                    "operator_identity": operator_identity,
                },
            )
        conn.commit()
        logger.info(
            "Audit record written: pipeline_run_id=%s status=%s",
            audit_record["pipeline_run_id"],
            audit_record["status"],
        )
    except Exception:
        # LOGIC — roll back and re-raise; caller decides whether to halt pipeline
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Failed to rollback audit connection", exc_info=True)
        logger.exception(
            "Failed to write audit record for pipeline_run_id=%s",
            audit_record.get("pipeline_run_id"),
        )
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                logger.warning("Failed to close audit connection", exc_info=True)


def _get_operator_identity() -> str:
    # LOGIC — retrieves IAM principal ARN from STS; no credentials in code
    try:
        sts_client = boto3.client("sts")
        identity = sts_client.get_caller_identity()
        arn = identity["Arn"]
        logger.debug("Operator identity resolved: %s", arn)
        return arn
    except Exception:
        logger.exception("Failed to retrieve caller identity from STS")
        raise