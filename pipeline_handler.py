# BOILERPLATE
import json
import logging
import os
import re

import file_parser
import row_validator
import db_loader
import report_writer
import error_writer
import audit_logger
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex anchored to the exact incoming key convention from the data contracts
_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def handler(event: dict, context: object) -> dict:
    """
    Lambda entry point.  Receives an S3 ObjectCreated event, orchestrates the
    full trade-positions pipeline, and returns a structured JSON response.
    """
    # BOILERPLATE — extract S3 coordinates from event
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event payload: %s", exc)
        raise

    logger.info("Received S3 event: bucket=%s key=%s", bucket, key)

    # LOGIC — validate key against expected pattern; reject early if it does not match
    match = _KEY_PATTERN.match(key)
    if not match:
        logger.warning(
            "Key '%s' does not match expected pattern "
            "'incoming/{desk_code}_{trade_date}_positions.csv'. Skipping.",
            key,
        )
        return {"statusCode": 200, "body": json.dumps({"skipped": True, "key": key})}

    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    filename = key

    logger.info("Processing file for desk_code=%s trade_date=%s", desk_code, trade_date)

    # LOGIC — pipeline state variables initialised to safe defaults for the failure path
    rows_inserted = 0
    rows_rejected = 0
    total_rows = 0
    report_s3_key = None
    error_s3_key = None
    manifest_s3_key = None

    try:
        # LOGIC — Step 1: parse raw CSV from S3
        raw_df = file_parser.parse_s3_csv(bucket, key)
        logger.info("Parsed %d raw rows from %s", len(raw_df), key)

        # LOGIC — Step 2: validate rows; split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date)
        rows_rejected = len(rejected_df)
        total_rows = len(valid_df) + rows_rejected
        logger.info(
            "Validation complete: valid=%d rejected=%d", len(valid_df), rows_rejected
        )

        # LOGIC — Step 3: load valid rows into DB (idempotent)
        rows_inserted = db_loader.load_positions(valid_df)
        logger.info("Rows inserted into DB: %d", rows_inserted)

        # LOGIC — Step 4: write summary report to S3
        report_s3_key = report_writer.write_summary(
            bucket, key, valid_df, rejected_df, rows_inserted
        )
        logger.info("Summary report written: %s", report_s3_key)

        # LOGIC — Step 5: write error file (rejected rows) to S3 — empty string if none
        error_s3_key = error_writer.write_error_file(bucket, key, rejected_df)
        if error_s3_key:
            logger.info("Error file written: %s", error_s3_key)
        else:
            logger.info("No rejected rows — error file not written")

        # LOGIC — Step 6: write manifest (predictable key → timestamped file keys)
        manifest_s3_key = report_writer.write_manifest(
            bucket, desk_code, trade_date, report_s3_key, error_s3_key
        )
        logger.info("Manifest written: %s", manifest_s3_key)

        # LOGIC — determine pipeline status for audit record
        if rows_rejected == 0:
            status = "SUCCESS"
        elif rows_inserted > 0:
            status = "PARTIAL"
        else:
            # All rows rejected or nothing inserted
            status = "PARTIAL" if total_rows > 0 else "SUCCESS"

        # LOGIC — Step 7: write audit record
        audit_logger.write_audit(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
        )

        # LOGIC — Step 8: publish success notification to SNS
        import pytz  # BOILERPLATE — imported here to keep top-level imports minimal
        from datetime import datetime

        et_now = datetime.now(pytz.timezone("America/Toronto")).isoformat()

        success_summary = {
            "event": "TRADE_POSITION_LOAD_SUCCESS",
            "filename": filename,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows": total_rows,
            "rows_loaded": rows_inserted,
            "rows_rejected": rows_rejected,
            "report_s3_key": report_s3_key,
            "manifest_s3_key": manifest_s3_key,
            "processing_timestamp_et": et_now,
        }
        sns_notifier.notify_success(success_summary)

        logger.info(
            "Pipeline completed successfully: status=%s inserted=%d rejected=%d",
            status,
            rows_inserted,
            rows_rejected,
        )

        # LOGIC — return structured response; QA tests parse body as JSON
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file": error_s3_key if error_s3_key else None,
            "report_file": report_s3_key,
        }
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:  # LOGIC — catch-all: audit + notify before re-raising
        logger.exception("Unhandled exception in pipeline for key=%s: %s", key, exc)

        import pytz  # BOILERPLATE
        from datetime import datetime

        et_now = datetime.now(pytz.timezone("America/Toronto")).isoformat()

        error_dict = {
            "event": "TRADE_POSITION_LOAD_FAILURE",
            "filename": filename,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "error_message": str(exc),
            "processing_timestamp_et": et_now,
        }

        # LOGIC — best-effort failure notification; do not suppress original exception
        try:
            sns_notifier.notify_failure(error_dict)
        except Exception as notify_exc:
            logger.error("Failed to publish failure notification: %s", notify_exc)

        # LOGIC — best-effort audit write on failure path
        try:
            audit_logger.write_audit(
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=str(exc),
            )
        except Exception as audit_exc:
            logger.error("Failed to write failure audit record: %s", audit_exc)

        raise