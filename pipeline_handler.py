# BOILERPLATE
import logging
import os

import time_utils
import file_parser
import row_validator
import db_loader
import report_writer
import error_writer
import audit_writer
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — Lambda entry point
    processing_timestamp = time_utils.now_et()
    filename = None
    desk_code = None
    trade_date = None

    try:
        bucket, key = _extract_s3_key(event)
        filename = key
        logger.info("Received S3 event: bucket=%s key=%s", bucket, key)

        summary = _run_pipeline(bucket, key)

        logger.info(
            "Pipeline completed successfully: filename=%s rows_loaded=%s rows_rejected=%s",
            filename,
            summary.get("rows_loaded"),
            summary.get("rows_rejected"),
        )
        return {"statusCode": 200, "body": "OK"}

    except Exception as exc:  # LOGIC — failure path: audit + notify before re-raise
        error_message = str(exc)
        logger.error(
            "Pipeline failed: filename=%s error=%s",
            filename,
            error_message,
            exc_info=True,
        )

        # LOGIC — write failure audit record; best-effort, do not suppress the original exception
        try:
            audit_writer.write_audit(
                filename=filename if filename is not None else "unknown",
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILURE",
                total_rows=0,
                rows_inserted=0,
                rows_rejected=0,
                error_message=error_message,
                processing_timestamp_et=processing_timestamp,
            )
        except Exception as audit_exc:
            logger.error("Failed to write failure audit record: %s", audit_exc, exc_info=True)

        # LOGIC — publish failure SNS notification; best-effort
        try:
            sns_notifier.notify_failure(
                filename=filename if filename is not None else "unknown",
                error_message=error_message,
                processing_timestamp_et=processing_timestamp,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish failure SNS notification: %s", sns_exc, exc_info=True)

        return {"statusCode": 500, "body": error_message}


def _extract_s3_key(event: dict) -> tuple:
    # LOGIC — extract bucket and key from S3 event structure
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    return bucket, key


def _run_pipeline(bucket: str, key: str) -> dict:
    # LOGIC — full pipeline orchestration; returns summary dict on success
    processing_timestamp = time_utils.now_et()
    logger.info("Starting pipeline run: bucket=%s key=%s timestamp_et=%s", bucket, key, processing_timestamp)

    # Step 1: Parse S3 file and extract metadata from filename
    logger.info("Step 1: Parsing S3 file")
    raw_df, desk_code, trade_date = file_parser.parse_s3_file(bucket, key)
    filename = key
    logger.info(
        "Parsed file: desk_code=%s trade_date=%s rows=%d",
        desk_code,
        trade_date,
        len(raw_df),
    )

    # Step 2: Validate rows
    logger.info("Step 2: Validating rows")
    valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date)
    logger.info(
        "Validation complete: valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    # Step 3: Write rejected rows to S3 error file
    logger.info("Step 3: Writing error file (if any rejections)")
    error_s3_key = error_writer.write_errors(
        rejected_df=rejected_df,
        desk_code=desk_code,
        trade_date=trade_date,
        processing_timestamp=processing_timestamp,
    )
    if error_s3_key:
        logger.info("Error file written: %s", error_s3_key)
    else:
        logger.info("No rejected rows — error file not written")

    # Step 4: Load valid rows into Aurora
    logger.info("Step 4: Loading valid rows into Aurora")
    rows_inserted = db_loader.load_positions(valid_df)
    logger.info("Rows inserted: %d", rows_inserted)

    # Step 5: Generate and write summary report + manifest
    logger.info("Step 5: Writing summary report and manifest")
    summary_dict, report_s3_key = report_writer.write_report(
        valid_df=valid_df,
        rejected_df=rejected_df,
        rows_inserted=rows_inserted,
        desk_code=desk_code,
        trade_date=trade_date,
        filename=filename,
        processing_timestamp=processing_timestamp,
        error_s3_key=error_s3_key,
    )
    logger.info("Report written: %s", report_s3_key)

    # Step 6: Write success audit record
    logger.info("Step 6: Writing audit record")
    total_rows = len(valid_df) + len(rejected_df)
    audit_writer.write_audit(
        filename=filename,
        desk_code=desk_code,
        trade_date=trade_date,
        status="SUCCESS",
        total_rows=total_rows,
        rows_inserted=rows_inserted,
        rows_rejected=len(rejected_df),
        error_message=None,
        processing_timestamp_et=processing_timestamp,
    )
    logger.info("Audit record written")

    # Step 7: Publish success SNS notification
    logger.info("Step 7: Publishing success SNS notification")
    sns_payload = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": str(trade_date),
        "processing_timestamp_et": time_utils.format_et(processing_timestamp),
        "total_rows": total_rows,
        "rows_loaded": rows_inserted,
        "rows_rejected": len(rejected_df),
        "report_s3_key": report_s3_key,
    }
    sns_notifier.notify_success(sns_payload)
    logger.info("Success SNS notification published")

    return sns_payload