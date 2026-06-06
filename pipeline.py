# BOILERPLATE
import logging
import json
from datetime import datetime
from decimal import Decimal

import boto3
import pytz

from config import Config
import secrets as app_secrets
import file_reader
import validator
import error_writer
import loader
import report_builder
import notifier
import audit

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")  # LOGIC — single timezone constant per BAC-7


def run_pipeline(s3_key: str) -> None:  # LOGIC — orchestrates end-to-end pipeline
    """
    Orchestrates the full position-file processing pipeline for a single S3 file.
    Steps follow the approved design execution sequence exactly.
    """
    # Step 1 — resolve ET timestamp once; thread through all downstream calls (BAC-7)
    # LOGIC
    processed_at: datetime = datetime.now(ET)
    logger.info("Pipeline started. s3_key=%s processed_at=%s", s3_key, processed_at.isoformat())

    # Step 2 — load config from environment variables (BAC-8)
    # BOILERPLATE
    cfg = Config()

    # Initialise AWS clients — use existing services only, never create/provision
    # BOILERPLATE
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # Mutable state accumulated through the pipeline; used in the except block
    # LOGIC
    desk_code: str = ""
    trade_date: str = ""
    total_rows: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    db_conn = None

    try:
        # Step 3 — retrieve DB credentials from Secrets Manager (BAC-8)
        # LOGIC
        db_credentials = app_secrets.get_db_credentials(cfg.db_secret_id)
        logger.info("DB credentials retrieved from Secrets Manager.")

        # Step 4 — read position file from S3
        # LOGIC
        raw_df, desk_code, trade_date = file_reader.read_position_file(
            s3_client, cfg.s3_bucket, s3_key
        )
        total_rows = len(raw_df)
        logger.info(
            "File read. desk_code=%s trade_date=%s total_rows=%d",
            desk_code,
            trade_date,
            total_rows,
        )

        # Step 5 — validate rows; split into valid and rejected DataFrames
        # LOGIC
        valid_df, rejected_df = validator.validate_rows(raw_df, desk_code, trade_date)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete. valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # Step 6 — write error file if any rejections exist (BAC-2)
        # LOGIC
        error_s3_key = error_writer.write_error_file(
            s3_client,
            rejected_df,
            cfg.s3_bucket,
            cfg.s3_error_prefix,
            desk_code,
            trade_date,
            processed_at,
        )
        if error_s3_key:
            logger.info("Error file written. s3_key=%s", error_s3_key)

        # Step 7 — open DB connection
        # LOGIC
        db_conn = loader._build_connection(db_credentials)
        logger.info("Database connection established.")

        # Step 8 — bulk upsert valid rows; returns count of rows actually inserted (BAC-1, BAC-3)
        # LOGIC
        rows_loaded = loader.load_positions(valid_df, db_credentials, processed_at)
        logger.info(
            "Load complete. rows_inserted=%d rows_skipped=%d",
            rows_loaded,
            len(valid_df) - rows_loaded,
        )

        # Step 9 — build summary report (BAC-4)
        # LOGIC
        report = report_builder.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_loaded,
            desk_code=desk_code,
            trade_date=trade_date,
            processed_at=processed_at,
            s3_key_source=s3_key,
        )
        logger.info("Report built. rows_loaded=%d rows_rejected=%d", rows_loaded, rows_rejected)

        # Step 10 — write report JSON to S3 (BAC-4)
        # LOGIC
        report_s3_key = report_builder.write_report(
            s3_client,
            report,
            cfg.s3_bucket,
            cfg.s3_report_prefix,
            desk_code,
            trade_date,
            processed_at,
        )
        logger.info("Report written. s3_key=%s", report_s3_key)

        # Step 11 — publish success notification (BAC-5)
        # LOGIC
        notifier.notify_success(sns_client, cfg.sns_success_topic_arn, report)
        logger.info("Success notification published. topic_arn=%s", cfg.sns_success_topic_arn)

        # Step 12 — write success audit record (BAC-7, NFR 3.3)
        # LOGIC
        audit.write_audit_record(
            conn=db_conn,
            desk_code=desk_code,
            trade_date=trade_date,
            s3_key=s3_key,
            processing_service_id=cfg.processing_service_id,
            status="SUCCESS",
            total_rows=total_rows,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            error_message=None,
            processed_at=processed_at,
        )
        logger.info("Audit record written with status=SUCCESS.")

    except Exception as exc:  # LOGIC — top-level failure handler (step 13)
        logger.exception(
            "Pipeline failed. s3_key=%s desk_code=%s trade_date=%s error=%s",
            s3_key,
            desk_code,
            trade_date,
            str(exc),
        )

        # Publish failure notification (BAC-5)
        # LOGIC
        try:
            notifier.notify_failure(
                sns_client,
                cfg.sns_failure_topic_arn,
                desk_code=desk_code,
                trade_date=trade_date,
                s3_key=s3_key,
                error_message=f"{type(exc).__name__}: {exc}",
                processed_at=processed_at,
            )
        except Exception as notify_exc:
            logger.error("Failed to publish failure notification: %s", str(notify_exc))

        # Write failure audit record if a DB connection is available
        # LOGIC
        if db_conn is not None:
            try:
                audit.write_audit_record(
                    conn=db_conn,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    s3_key=s3_key,
                    processing_service_id=cfg.processing_service_id,
                    status="FAILURE",
                    total_rows=total_rows,
                    rows_loaded=rows_loaded,
                    rows_rejected=rows_rejected,
                    error_message=f"{type(exc).__name__}: {exc}",
                    processed_at=processed_at,
                )
                logger.info("Audit record written with status=FAILURE.")
            except Exception as audit_exc:
                logger.error("Failed to write failure audit record: %s", str(audit_exc))
        else:
            logger.warning(
                "No DB connection available; failure audit record could not be written. "
                "s3_key=%s",
                s3_key,
            )

        # Re-raise so the Lambda runtime marks the invocation as failed
        raise

    finally:
        # BOILERPLATE — always close the DB connection if one was opened
        if db_conn is not None:
            try:
                db_conn.close()
                logger.info("Database connection closed.")
            except Exception as close_exc:
                logger.error("Error closing DB connection: %s", str(close_exc))