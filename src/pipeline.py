import logging
import os
import uuid

import boto3
import pytz
from datetime import datetime
from sqlalchemy import create_engine

from src import (
    audit,
    error_writer,
    file_reader,
    loader,
    notifier,
    reporter,
    validator,
)
from src.config import (
    DB_NAME,
    DB_SECRET_ID,
    S3_BUCKET,
    SNS_FAILURE_TOPIC_ARN,
    SNS_SUCCESS_TOPIC_ARN,
)
from src.secrets import get_db_connection_string

# BOILERPLATE
logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/Toronto")


def run_pipeline(s3_key: str) -> dict:
    # BOILERPLATE — capture processing timestamp in ET at the very start.
    processing_ts: datetime = datetime.now(_ET)
    run_id = uuid.uuid4()

    # BOILERPLATE — boto3 clients; region read from AWS_REGION env var by boto3 automatically.
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # LOGIC — pre-declare metadata so the except block can reference them even if
    # extraction fails before they are assigned.
    desk_code: str = "UNKNOWN"
    trade_date: str = "UNKNOWN"

    # LOGIC — read SERVICE_IDENTITY from environment for audit trail.
    service_identity: str = os.environ["SERVICE_IDENTITY"]

    try:
        # BOILERPLATE — database engine created once for the whole pipeline run.
        connection_string = get_db_connection_string(DB_SECRET_ID, DB_NAME)
        engine = create_engine(connection_string, future=True)

        # Step 5 — extract metadata from S3 key.
        desk_code, trade_date = file_reader.extract_metadata_from_key(s3_key)
        logger.info(
            "Pipeline start: run_id=%s s3_key=%s desk_code=%s trade_date=%s",
            run_id,
            s3_key,
            desk_code,
            trade_date,
        )

        # Step 6 — download and parse CSV.
        raw_df, total_rows = file_reader.download_and_parse(s3_client, S3_BUCKET, s3_key)
        logger.info("File parsed: total_rows=%d s3_key=%s", total_rows, s3_key)

        # Step 7 — validate rows.
        valid_df, rejected_df = validator.validate(raw_df, desk_code, trade_date)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )
        if len(rejected_df) > 0:
            logger.warning(
                "Rejected rows detected: count=%d desk_code=%s trade_date=%s",
                len(rejected_df),
                desk_code,
                trade_date,
            )

        # Step 8 — write error file if there are rejected rows.
        error_s3_key = error_writer.write_error_file(
            s3_client, S3_BUCKET, rejected_df, desk_code, trade_date, processing_ts
        )
        if error_s3_key:
            logger.info("Error file written: %s", error_s3_key)

        # Step 9 — load valid positions into the database.
        rows_inserted = loader.load_positions(engine, valid_df, processing_ts)
        rows_skipped_duplicate = len(valid_df) - rows_inserted
        logger.info(
            "Load complete: rows_inserted=%d rows_skipped_duplicate=%d",
            rows_inserted,
            rows_skipped_duplicate,
        )

        # Step 10 — build the summary report dict.
        report = reporter.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_ts=processing_ts,
            error_s3_key=error_s3_key,
        )

        # Step 11 — write JSON report to S3.
        report_s3_key = reporter.write_report(
            s3_client, S3_BUCKET, report, desk_code, trade_date, processing_ts
        )
        logger.info("Report written: %s", report_s3_key)

        # Step 12 — write audit record.
        audit_row = {
            "run_id": str(run_id),
            "s3_key": s3_key,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "processing_timestamp": processing_ts,
            "status": report["status"],
            "total_rows": total_rows,
            "rows_inserted": rows_inserted,
            "rows_rejected": len(rejected_df),
            "rows_skipped_duplicate": rows_skipped_duplicate,
            "report_s3_key": report_s3_key,
            "error_s3_key": error_s3_key,
            "service_identity": service_identity,
        }
        audit.write_audit_record(engine, audit_row)

        # Step 13 — notify downstream systems of success.
        report["report_s3_key"] = report_s3_key
        notifier.notify_success(sns_client, SNS_SUCCESS_TOPIC_ARN, report)
        logger.info(
            "Pipeline complete: run_id=%s status=%s", run_id, report["status"]
        )

        return report

    except Exception as exc:  # LOGIC — catch-all for unhandled pipeline exceptions.
        logger.error(
            "Pipeline failed: run_id=%s s3_key=%s error=%s",
            run_id,
            s3_key,
            str(exc),
            exc_info=True,
        )

        # LOGIC — attempt failure notification; log but do not suppress secondary errors.
        try:
            notifier.notify_failure(
                sns_client,
                SNS_FAILURE_TOPIC_ARN,
                desk_code,
                trade_date,
                str(exc),
                processing_ts,
            )
        except Exception as notify_exc:
            logger.error(
                "Failed to send failure notification: %s", str(notify_exc), exc_info=True
            )

        # LOGIC — attempt to write ERROR audit record; log but do not suppress.
        try:
            error_audit_row = {
                "run_id": str(run_id),
                "s3_key": s3_key,
                "desk_code": desk_code,
                "trade_date": trade_date,
                "processing_timestamp": processing_ts,
                "status": "ERROR",
                "total_rows": 0,
                "rows_inserted": 0,
                "rows_rejected": 0,
                "rows_skipped_duplicate": 0,
                "report_s3_key": None,
                "error_s3_key": None,
                "service_identity": service_identity,
            }
            # LOGIC — engine may not exist if connection setup failed; guard accordingly.
            if "engine" in dir():
                audit.write_audit_record(engine, error_audit_row)
        except Exception as audit_exc:
            logger.error(
                "Failed to write error audit record: %s", str(audit_exc), exc_info=True
            )

        raise