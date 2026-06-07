# BOILERPLATE
import json
import logging
import os
import re

import pytz

from datetime import datetime

# BOILERPLATE — module imports (sibling modules, all expected to exist)
import file_reader
import row_validator
import db_loader
import error_file_writer
import report_writer
import sns_notifier
import secret_manager_client
import db_audit_writer

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern: incoming/{desk_code}_{trade_date}_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?:incoming/)?([A-Za-z0-9_-]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_filename(key: str) -> tuple:
    """
    # LOGIC
    Extract desk_code and trade_date from an S3 key of the form:
    incoming/{desk_code}_{trade_date}_positions.csv
    Returns (desk_code: str, trade_date: str).
    Raises ValueError if the key does not match the expected pattern.
    """
    match = _FILENAME_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"Filename does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv': {key!r}"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return desk_code, trade_date


def handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    Lambda entry point. Receives an S3 event trigger (object created under
    incoming/ prefix), orchestrates the full pipeline, and returns a structured
    response dict.
    """
    # BOILERPLATE — Eastern Time timezone reference
    et_tz = pytz.timezone("America/Toronto")

    # LOGIC — extract S3 event fields
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event payload: %s", exc)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Malformed event: {exc}"}),
        }

    logger.info("Pipeline triggered for bucket=%s key=%s", bucket, key)

    # LOGIC — validate filename pattern and extract metadata
    try:
        desk_code, trade_date = _parse_filename(key)
    except ValueError as exc:
        logger.error("Filename validation failed: %s", exc)
        sns_notifier.notify_failure(
            error_type="ValueError",
            error_message=str(exc),
            source_file_key=key,
        )
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(exc)}),
        }

    logger.info("Parsed desk_code=%s trade_date=%s", desk_code, trade_date)

    # LOGIC — service identity for audit trail (Lambda function name + request ID)
    service_identity = (
        f"{getattr(context, 'function_name', 'unknown')}"
        f":{getattr(context, 'aws_request_id', 'unknown')}"
    )

    # BOILERPLATE — obtain DB connection (credentials from Secrets Manager)
    db_conn = None
    audit_id = None

    try:
        db_conn = secret_manager_client.get_db_connection()
    except Exception as exc:  # LOGIC — credential failure is a hard stop
        logger.error("Failed to obtain DB connection: %s", exc)
        sns_notifier.notify_failure(
            error_type=type(exc).__name__,
            error_message=str(exc),
            source_file_key=key,
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"DB connection failure: {exc}"}),
        }

    # LOGIC — write STARTED audit record before any processing
    try:
        audit_id = db_audit_writer.start_audit_record(
            db_conn=db_conn,
            source_file_key=key,
            desk_code=desk_code,
            trade_date=trade_date,
            service_identity=service_identity,
        )
        logger.info("Audit record started with audit_id=%s", audit_id)
    except Exception as exc:
        logger.error("Failed to write start audit record: %s", exc)
        # Non-fatal: continue pipeline, audit trail best-effort
        audit_id = None

    # LOGIC — main pipeline execution block
    try:
        # Step 1: Read the CSV from S3
        logger.info("Reading position file from S3: s3://%s/%s", bucket, key)
        raw_df, total_row_count = file_reader.read_position_file(
            bucket=bucket, key=key
        )
        logger.info("File read complete: total_row_count=%d", total_row_count)

        # Step 2: Validate rows
        logger.info("Validating rows with desk_code=%s", desk_code)
        valid_df, rejected_df = row_validator.validate_rows(
            raw_df=raw_df,
            filename_desk_code=desk_code,
        )
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # Step 3: Load valid rows into the database
        logger.info("Loading %d valid rows into trade_positions", len(valid_df))
        rows_inserted = db_loader.load_positions(
            valid_df=valid_df,
            db_conn=db_conn,
        )
        logger.info("DB load complete: rows_inserted=%d", rows_inserted)

        # Step 4: Write error file for rejected rows (only if rejections exist)
        error_s3_key = error_file_writer.write_error_file(
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            bucket=bucket,
        )
        if error_s3_key:
            logger.info("Error file written to s3://%s/%s", bucket, error_s3_key)
        else:
            logger.info("No rejected rows — error file not written")

        # Step 5: Write summary report to S3
        logger.info("Writing summary report to S3")
        report = report_writer.write_summary_report(
            total_rows=total_row_count,
            rows_loaded=rows_inserted,
            rejected_df=rejected_df,
            valid_df=valid_df,
            desk_code=desk_code,
            trade_date=trade_date,
            source_file_key=key,
            bucket=bucket,
        )
        logger.info("Report written: %s", report)

        # Step 6: Publish success notification to SNS
        sns_notifier.notify_success(report=report)
        logger.info("Success notification published to SNS")

        # LOGIC — update audit record with SUCCESS status
        if audit_id is not None:
            try:
                db_audit_writer.complete_audit_record(
                    db_conn=db_conn,
                    audit_id=audit_id,
                    status="SUCCESS",
                    total_rows=total_row_count,
                    rows_loaded=rows_inserted,
                    rows_rejected=len(rejected_df),
                    error_message=None,
                )
                logger.info("Audit record completed with status=SUCCESS")
            except Exception as audit_exc:
                logger.error(
                    "Failed to complete audit record (non-fatal): %s", audit_exc
                )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "status": "SUCCESS",
                    "desk_code": desk_code,
                    "trade_date": trade_date,
                    "total_rows": total_row_count,
                    "rows_loaded": rows_inserted,
                    "rows_rejected": len(rejected_df),
                }
            ),
        }

    except Exception as exc:  # LOGIC — catch all unhandled pipeline errors
        logger.exception(
            "Pipeline failed for key=%s: %s: %s", key, type(exc).__name__, exc
        )

        # LOGIC — publish failure notification
        try:
            sns_notifier.notify_failure(
                error_type=type(exc).__name__,
                error_message=str(exc),
                source_file_key=key,
            )
            logger.info("Failure notification published to SNS")
        except Exception as notify_exc:
            logger.error(
                "Failed to publish failure notification (non-fatal): %s", notify_exc
            )

        # LOGIC — update audit record with FAILURE status
        if audit_id is not None:
            try:
                db_audit_writer.complete_audit_record(
                    db_conn=db_conn,
                    audit_id=audit_id,
                    status="FAILURE",
                    total_rows=0,
                    rows_loaded=0,
                    rows_rejected=0,
                    error_message=str(exc),
                )
                logger.info("Audit record completed with status=FAILURE")
            except Exception as audit_exc:
                logger.error(
                    "Failed to complete failure audit record (non-fatal): %s",
                    audit_exc,
                )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "status": "FAILURE",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "source_file_key": key,
                }
            ),
        }

    finally:
        # BOILERPLATE — close DB connection
        if db_conn is not None:
            try:
                db_conn.close()
                logger.info("DB connection closed")
            except Exception as close_exc:
                logger.warning("Error closing DB connection: %s", close_exc)