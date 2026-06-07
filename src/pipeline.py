# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import boto3
import psycopg2
import pytz

from src import (  # BOILERPLATE
    auditor,
    error_writer,
    file_reader,
    loader,
    notifier,
    reporter,
    secrets,
    validator,
)
from src.auditor import AuditRecord
from src.config import Config

# BOILERPLATE
logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE

# LOGIC — filename pattern defined in the data contract
_S3_KEY_RE = re.compile(
    r"^incoming/([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_s3_key(s3_key: str):
    """
    # LOGIC
    Extracts desk_code and trade_date from the S3 key.
    Raises ValueError if the key does not match the expected pattern.
    """
    match = _S3_KEY_RE.match(s3_key)
    if not match:
        raise ValueError(
            f"S3 key '{s3_key}' does not match expected pattern "
            r"'^incoming/([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$'"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return desk_code, trade_date


def run_pipeline(s3_key: str, cfg: Config) -> dict:
    """
    # LOGIC
    Orchestrates the full processing sequence for a single S3 position file.

    Steps:
        1. Parse desk_code and trade_date from s3_key.
        2. Instantiate boto3 clients.
        3. Retrieve DB credentials from Secrets Manager.
        4. Open psycopg2 connection.
        5. Read file from S3.
        6. Validate rows.
        7. Load valid rows into Aurora.
        8. Write rejected rows to S3 error file.
        9. Build and write summary report.
        10. Publish success notification.
    On any exception: publish failure notification, then write audit record
    and re-raise.
    Always: write audit record in finally block.
    """
    # LOGIC — capture one consistent ET timestamp for the entire run
    processing_ts: datetime = datetime.now(ET)

    # LOGIC — parse key first; a bad key is an immediate, unrecoverable error
    desk_code, trade_date = _parse_s3_key(s3_key)

    logger.info(
        "Pipeline starting: s3_key=%s desk_code=%s trade_date=%s",
        s3_key,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — boto3 clients
    s3_client = boto3.client("s3", region_name=cfg.AWS_REGION)
    sns_client = boto3.client("sns", region_name=cfg.AWS_REGION)

    # LOGIC — mutable state accumulated across pipeline steps
    summary: Optional[dict] = None
    outcome: str = "FAILURE"
    error_detail: Optional[str] = None
    total_rows_received: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    rows_skipped_duplicate: int = 0

    # BOILERPLATE — DB connection, closed in finally regardless of outcome
    db_creds = secrets.get_db_credentials(cfg.DB_SECRET_ID)
    conn = psycopg2.connect(
        host=db_creds.host,
        port=db_creds.port,
        dbname=db_creds.dbname,
        user=db_creds.username,
        password=db_creds.password,
    )

    try:
        # LOGIC — Step 1: read CSV from S3
        logger.info("Reading position file from S3: key=%s", s3_key)
        raw_df, total_rows_received = file_reader.read_position_file(
            s3_client, cfg.S3_BUCKET, s3_key
        )

        # LOGIC — Step 2: validate rows
        logger.info(
            "Validating %d rows for desk_code=%s trade_date=%s",
            total_rows_received,
            desk_code,
            trade_date,
        )
        valid_df, rejected_df = validator.validate_positions(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — Step 3: load valid rows into Aurora
        logger.info(
            "Loading %d valid rows into demo_schema.trade_positions", len(valid_df)
        )
        rows_loaded = loader.load_positions(conn, valid_df)
        rows_skipped_duplicate = len(valid_df) - rows_loaded
        logger.info(
            "Load complete: rows_inserted=%d rows_skipped_duplicate=%d",
            rows_loaded,
            rows_skipped_duplicate,
        )

        # LOGIC — Step 4: write rejected rows to S3 error file (no-op if empty)
        error_s3_key = error_writer.write_error_file(
            s3_client, cfg.S3_BUCKET, desk_code, trade_date, rejected_df
        )
        if error_s3_key:
            logger.info("Error file written to S3: key=%s", error_s3_key)
        else:
            logger.info("No rejected rows; error file not written.")

        # LOGIC — Step 5: build summary report dict
        summary = reporter.build_summary(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_loaded,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_ts=processing_ts,
        )

        # LOGIC — Step 6: upload summary JSON to S3
        report_s3_key = reporter.write_report(
            s3_client, cfg.S3_BUCKET, desk_code, trade_date, summary
        )
        logger.info("Summary report written to S3: key=%s", report_s3_key)

        # LOGIC — Step 7: publish success SNS notification
        notifier.notify_success(sns_client, cfg.SNS_SUCCESS_ARN, summary)
        logger.info("Success notification published to SNS.")

        outcome = "SUCCESS"
        logger.info(
            "Pipeline completed successfully: s3_key=%s rows_loaded=%d",
            s3_key,
            rows_loaded,
        )

    except Exception as exc:
        # LOGIC — capture error detail for audit and failure notification
        error_detail = str(exc)
        logger.exception(
            "Pipeline failed for s3_key=%s: %s", s3_key, error_detail
        )

        # LOGIC — publish failure notification before re-raising
        try:
            notifier.notify_failure(
                sns_client,
                cfg.SNS_FAILURE_ARN,
                desk_code,
                trade_date,
                s3_key,
                error_detail,
            )
        except Exception:
            # LOGIC — notification failure must not suppress the original error
            logger.exception(
                "Failed to publish failure notification for s3_key=%s", s3_key
            )

        raise  # LOGIC — re-raise after audit write in finally

    finally:
        # LOGIC — audit record is always written, success or failure
        service_identity = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "local")

        audit_record = AuditRecord(
            s3_key=s3_key,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_timestamp_et=processing_ts,
            outcome=outcome,
            total_rows_received=total_rows_received,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            rows_skipped_duplicate=rows_skipped_duplicate,
            error_detail=error_detail,
            service_identity=service_identity,
        )

        try:
            auditor.write_audit_record(conn, audit_record)
        except Exception:
            # LOGIC — audit failure is logged but does not suppress original error
            logger.exception(
                "Failed to write audit record for s3_key=%s", s3_key
            )
        finally:
            # BOILERPLATE — always close the DB connection
            try:
                conn.close()
                logger.info("DB connection closed for s3_key=%s", s3_key)
            except Exception:
                logger.exception(
                    "Error closing DB connection for s3_key=%s", s3_key
                )

    return summary  # LOGIC — only reached on success path