# BOILERPLATE
import logging
import os
import re

import pytz
from datetime import datetime

import file_reader
import row_validator
import error_writer
import position_loader
import report_builder
import sns_notifier
import audit_logger
import secret_loader

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — Eastern Time zone used for all timestamps
ET = pytz.timezone("America/Toronto")


def parse_s3_event(event: dict) -> tuple:
    # LOGIC — extract bucket and key from the S3 trigger event
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as exc:
        raise ValueError(f"Malformed S3 event structure: {exc}") from exc

    logger.info("Parsed S3 event: bucket=%s key=%s", bucket, key)
    return bucket, key


def parse_filename(key: str) -> tuple:
    # LOGIC — validate filename matches expected convention and extract metadata
    # Strip any path prefix (e.g. "incoming/") before matching
    basename = key.split("/")[-1]

    pattern = r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
    match = re.match(pattern, basename)

    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match expected pattern "
            f"'{{desk_code}}_{{YYYY-MM-DD}}_positions.csv'"
        )

    desk_code = match.group(1)
    trade_date = match.group(2)
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def run_pipeline(bucket: str, key: str) -> dict:
    # LOGIC — top-level orchestrator; calls all sub-modules in order
    desk_code, trade_date = parse_filename(key)
    s3_bucket = os.environ["S3_BUCKET"]

    # LOGIC — establish DB connection once for this pipeline run
    conn = secret_loader.get_db_connection()

    # LOGIC — write audit start record; capture audit_id for later update
    audit_id = audit_logger.write_audit_start(
        conn=conn,
        s3_key=key,
        desk_code=desk_code,
        trade_date=trade_date,
    )
    logger.info("Audit record started: audit_id=%d", audit_id)

    rows_received = 0
    rows_loaded = 0
    rows_rejected = 0
    rows_skipped = 0

    try:
        # LOGIC — step 1: read and parse CSV from S3
        raw_df = file_reader.read_position_file(bucket=bucket, key=key)
        rows_received = len(raw_df)
        logger.info("File read complete: %d rows received", rows_received)

        # LOGIC — step 2: validate rows; split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(
            df=raw_df,
            expected_desk_code=desk_code,
            expected_trade_date=trade_date,
        )
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: %d valid rows, %d rejected rows",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — step 3: write rejected rows to S3 error file (no-op if none)
        error_key = error_writer.write_error_file(
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            bucket=s3_bucket,
        )
        if error_key:
            logger.info("Error file written: %s", error_key)

        # LOGIC — step 4: load valid rows into trade_positions (idempotent)
        rows_loaded = position_loader.load_positions(valid_df=valid_df, conn=conn)
        rows_skipped = len(valid_df) - rows_loaded
        logger.info(
            "Load complete: %d rows inserted, %d rows skipped (duplicate)",
            rows_loaded,
            rows_skipped,
        )

        # LOGIC — step 5: build processing summary report
        report = report_builder.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_loaded,
            desk_code=desk_code,
            trade_date=trade_date,
        )

        # LOGIC — step 6: write report JSON to S3
        report_key = report_builder.write_report(
            report=report,
            desk_code=desk_code,
            trade_date=trade_date,
            bucket=s3_bucket,
        )
        logger.info("Report written: %s", report_key)

        # LOGIC — step 7: publish success SNS notification
        sns_notifier.publish_success(report=report)
        logger.info("Success notification published")

        # LOGIC — step 8: update audit record with SUCCESS status
        audit_logger.write_audit_complete(
            conn=conn,
            audit_id=audit_id,
            rows_received=rows_received,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            rows_skipped=rows_skipped,
            status="SUCCESS",
            error_message=None,
        )
        logger.info("Audit record completed: audit_id=%d status=SUCCESS", audit_id)

        conn.close()

        # LOGIC — build summary dict returned to caller
        summary = {
            "desk_code": desk_code,
            "trade_date": trade_date,
            "rows_received": rows_received,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
            "rows_skipped_duplicate": rows_skipped,
        }
        return summary

    except Exception as exc:  # LOGIC — catch-all: notify failure, update audit, re-raise
        error_message = str(exc)
        logger.error(
            "Pipeline failed for key=%s: %s", key, error_message, exc_info=True
        )

        # LOGIC — publish failure notification before re-raising
        try:
            sns_notifier.publish_failure(
                desk_code=desk_code,
                trade_date=trade_date,
                error_message=error_message,
                s3_key=key,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        # LOGIC — update audit record with FAILED status
        try:
            audit_logger.write_audit_complete(
                conn=conn,
                audit_id=audit_id,
                rows_received=rows_received,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
                rows_skipped=rows_skipped,
                status="FAILED",
                error_message=error_message,
            )
        except Exception as audit_exc:
            logger.error("Failed to update audit record on failure: %s", audit_exc)

        try:
            conn.close()
        except Exception:
            pass

        raise


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — AWS Lambda entry point
    logger.info(
        "Lambda invoked at %s ET",
        datetime.now(ET).isoformat(),
    )

    try:
        bucket, key = parse_s3_event(event)
    except ValueError as exc:
        logger.error("Failed to parse S3 event: %s", exc)
        return {"statusCode": 400, "body": f"Invalid S3 event: {exc}"}

    try:
        summary = run_pipeline(bucket=bucket, key=key)
        logger.info("Pipeline completed successfully: %s", summary)
        return {
            "statusCode": 200,
            "body": (
                f"Pipeline completed for {summary['desk_code']} "
                f"{summary['trade_date']}: "
                f"{summary['rows_loaded']} loaded, "
                f"{summary['rows_rejected']} rejected, "
                f"{summary['rows_skipped_duplicate']} skipped"
            ),
        }
    except ValueError as exc:
        # LOGIC — filename or structural validation failure — non-retryable
        logger.error("Pipeline validation error: %s", exc)
        return {"statusCode": 400, "body": f"Validation error: {exc}"}
    except Exception as exc:
        # LOGIC — unexpected failure — return 500 so Lambda marks invocation failed
        logger.error("Pipeline execution error: %s", exc, exc_info=True)
        return {"statusCode": 500, "body": f"Pipeline error: {exc}"}