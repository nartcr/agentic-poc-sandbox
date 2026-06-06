# BOILERPLATE
import datetime
import json
import logging
import uuid

import boto3
import pytz

from src.config import Config
from src.secrets import get_db_credentials
from src.file_reader import download_file, parse_csv
from src.validator import validate_rows
from src.loader import load_trades
from src.error_writer import write_error_file
from src.reporter import build_report, write_report
from src.notifier import publish_success, publish_failure
from src.auditor import record_audit

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone constant; used for every timestamp in this module
ET = pytz.timezone("America/Toronto")


def run_pipeline(s3_key: str) -> dict:
    # LOGIC — orchestrates end-to-end ingestion; must always publish SNS and write audit on failure
    started_at = datetime.datetime.now(ET)  # LOGIC: step 1 — wall-clock start in ET
    pipeline_run_id = str(uuid.uuid4())     # LOGIC: step 2 — unique run identifier

    logger.info(
        "Pipeline started: pipeline_run_id=%s s3_key=%s started_at=%s",
        pipeline_run_id,
        s3_key,
        started_at.isoformat(),
    )

    # BOILERPLATE — step 3: load config from environment variables
    config = Config()

    # BOILERPLATE — step 4: build boto3 clients against existing AWS services
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # BOILERPLATE — step 5: fetch DB credentials from Secrets Manager at runtime
    credentials = get_db_credentials(config.db_secret_id)

    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    error_file_key = None
    report = None

    try:
        # LOGIC — step 6: download source CSV from S3
        file_bytes = download_file(s3_client, config.s3_bucket, s3_key)
        logger.info("Downloaded S3 object: s3://%s/%s", config.s3_bucket, s3_key)

        # LOGIC — step 7: parse CSV into raw DataFrame; all columns as strings
        raw_df = parse_csv(file_bytes, s3_key)
        logger.info(
            "Parsed CSV: %d rows from %s", len(raw_df), s3_key
        )

        # LOGIC — step 8: validate rows; split into valid and rejected sets
        valid_df, rejected_df = validate_rows(raw_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 9: load valid rows into DB; capture ET timestamp immediately before insert
        loaded_at = datetime.datetime.now(ET)
        rows_inserted = load_trades(
            valid_df,
            credentials,
            s3_key,
            loaded_at=loaded_at,
        )
        logger.info(
            "DB load complete: rows_inserted=%d (of %d valid)",
            rows_inserted,
            len(valid_df),
        )

        # LOGIC — step 10: write error file to S3 only if there are rejections
        if len(rejected_df) > 0:
            error_file_key = write_error_file(
                s3_client,
                config.s3_bucket,
                config.s3_errors_prefix,
                rejected_df,
                s3_key,
            )
            logger.info("Error file written: %s", error_file_key)
        else:
            error_file_key = None

        # LOGIC — step 11: build summary report dict from all processing artefacts
        report = build_report(
            source_key=s3_key,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            load_timestamp=loaded_at,
        )
        # LOGIC — inject error_file_key into report (None if no rejections)
        report["error_file_key"] = error_file_key

        # LOGIC — step 12: write JSON report to S3
        report_key = write_report(
            s3_client,
            config.s3_bucket,
            config.s3_reports_prefix,
            report,
            s3_key,
        )
        logger.info("Report written: %s", report_key)

        # LOGIC — inject report_key into report so downstream consumers (SNS, audit) can reference it
        report["report_key"] = report_key

        # LOGIC — step 13: publish success SNS notification
        message_id = publish_success(
            sns_client,
            config.sns_success_topic_arn,
            report,
        )
        logger.info("Success SNS published: message_id=%s", message_id)

        # LOGIC — step 14: record completed_at after all work (before audit)
        completed_at = datetime.datetime.now(ET)

        # LOGIC — step 15: write audit record to app.pipeline_audit
        audit_record = _build_audit_record(
            source_file=s3_key,
            pipeline_run_id=pipeline_run_id,
            status="SUCCESS",
            total_rows_received=len(raw_df),
            rows_loaded=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
            started_at=started_at,
            completed_at=completed_at,
        )
        try:
            record_audit(credentials, audit_record)
        except Exception:
            # LOGIC — audit failure is logged but does NOT prevent returning the report
            logger.exception(
                "Audit write failed for pipeline_run_id=%s; result already delivered",
                pipeline_run_id,
            )

        logger.info(
            "Pipeline completed successfully: pipeline_run_id=%s rows_inserted=%d",
            pipeline_run_id,
            rows_inserted,
        )

        # LOGIC — step 16: return the report dict to the caller (handler.py)
        return report

    except Exception as exc:
        # LOGIC — top-level failure path: always publish failure SNS, always write audit
        logger.exception(
            "Pipeline failed: pipeline_run_id=%s s3_key=%s error=%s",
            pipeline_run_id,
            s3_key,
            str(exc),
        )

        failed_at = datetime.datetime.now(ET)

        # LOGIC — publish failure SNS; wrap to avoid masking original exception
        try:
            publish_failure(
                sns_client,
                config.sns_failure_topic_arn,
                s3_key,
                str(exc),
                failed_at,
            )
            logger.info("Failure SNS published for pipeline_run_id=%s", pipeline_run_id)
        except Exception:
            logger.exception(
                "Failed to publish failure SNS for pipeline_run_id=%s", pipeline_run_id
            )

        # LOGIC — write failure audit record; wrap to avoid masking original exception
        completed_at = datetime.datetime.now(ET)
        failure_audit_record = _build_audit_record(
            source_file=s3_key,
            pipeline_run_id=pipeline_run_id,
            status="FAILURE",
            total_rows_received=len(raw_df) if raw_df is not None else None,
            rows_loaded=rows_inserted,
            rows_rejected=len(rejected_df) if rejected_df is not None else None,
            error_message=str(exc),
            started_at=started_at,
            completed_at=completed_at,
        )
        try:
            record_audit(credentials, failure_audit_record)
        except Exception:
            logger.exception(
                "Failed to write failure audit record for pipeline_run_id=%s",
                pipeline_run_id,
            )

        raise


def _build_audit_record(
    source_file: str,
    pipeline_run_id: str,
    status: str,
    total_rows_received,
    rows_loaded: int,
    rows_rejected,
    error_message,
    started_at: datetime.datetime,
    completed_at: datetime.datetime,
) -> dict:
    # LOGIC — constructs the audit_record dict consumed by record_audit()
    return {
        "source_file": source_file,
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "total_rows_received": total_rows_received,
        "rows_loaded": rows_loaded,
        "rows_rejected": rows_rejected,
        "error_message": error_message,
        "started_at": started_at,
        "completed_at": completed_at,
    }