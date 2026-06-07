# BOILERPLATE
import logging
import os
import re
import urllib.parse
from datetime import datetime

# BOILERPLATE — sibling module imports (flat Lambda deployment layout)
import db_loader
import error_writer
import file_reader
import notifier
import report_builder
import row_validator
import time_utils
from pipeline_exceptions import FilenameParseError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def parse_s3_event(event: dict) -> tuple:
    """Extract bucket name and object key from an S3 event notification.

    Returns:
        (bucket, key) as strings.
    Raises:
        KeyError: if the event payload does not contain the expected structure.
    """
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    # S3 keys in event notifications are URL-encoded; decode so downstream
    # code receives the plain key string.
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    logger.info("Parsed S3 event: bucket=%s key=%s", bucket, key)
    return bucket, key


# LOGIC
def parse_filename(key: str) -> tuple:
    """Validate and decompose the object key into desk_code and trade_date_str.

    Expected key pattern:
        incoming/{desk_code}_{trade_date}_positions.csv

    desk_code  — one or more alphanumeric or hyphen characters
    trade_date — YYYY-MM-DD (strictly enforced by regex)

    Returns:
        (desk_code, trade_date_str)
    Raises:
        FilenameParseError: if the key does not match the expected pattern.
    """
    # LOGIC — extract just the filename portion of the key
    filename = key.split("/")[-1]

    # LOGIC — strict regex: desk_code allows letters, digits, hyphens;
    #          trade_date must be exactly YYYY-MM-DD; suffix is literal.
    pattern = r"^([A-Za-z0-9\-]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
    match = re.match(pattern, filename)
    if not match:
        raise FilenameParseError(
            f"Filename '{filename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'. "
            f"Full key: '{key}'"
        )

    desk_code = match.group(1)
    trade_date_str = match.group(2)
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date_str)
    return desk_code, trade_date_str


# LOGIC
def handler(event: dict, context: object) -> dict:
    """Lambda entry point.

    Orchestrates the full pipeline:
        1. Parse S3 event to get bucket + key.
        2. Parse filename to get desk_code + trade_date.
        3. Write PROCESSING audit record.
        4. Download & parse CSV from S3.
        5. Validate rows → valid_df, rejected_df.
        6. Load valid rows to database.
        7. Write error file if there are rejections.
        8. Build and write summary report.
        9. Publish success SNS notification.
       10. Update audit record to SUCCESS.

    On any unhandled exception:
        - Update audit record to FAILED (if audit_id exists).
        - Publish failure SNS notification.
        - Return {"statusCode": 500, "body": str(e)}.
    """
    # BOILERPLATE — capture pipeline start timestamp once; reused throughout
    processing_timestamp_et: datetime = time_utils.now_et()

    bucket: str = ""
    key: str = ""
    filename: str = ""
    desk_code: str | None = None
    trade_date_str: str | None = None
    audit_id: int | None = None
    conn = None

    try:
        # LOGIC — Step 1: parse the triggering S3 event
        bucket, key = parse_s3_event(event)
        filename = key.split("/")[-1]

        # LOGIC — Step 2: validate and decompose the filename
        desk_code, trade_date_str = parse_filename(key)

        # LOGIC — Step 3: open DB connection and write PROCESSING audit row
        conn = db_loader.get_db_connection()
        audit_id = db_loader.write_audit_record(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date_str,
            status="PROCESSING",
            total_rows=0,
            rows_inserted=0,
            rows_rejected=0,
            error_message=None,
            processing_timestamp_et=processing_timestamp_et,
        )
        logger.info("Audit record created: audit_id=%s status=PROCESSING", audit_id)

        # LOGIC — Step 4: download and parse CSV from S3
        raw_df = file_reader.download_and_parse(bucket=bucket, key=key)
        total_rows = len(raw_df)
        logger.info("Downloaded file: total_rows=%d", total_rows)

        # LOGIC — Step 5: validate rows
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — Step 6: load valid rows to database
        rows_inserted = db_loader.load_positions(valid_df=valid_df, conn=conn)
        logger.info("DB load complete: rows_inserted=%d", rows_inserted)

        # LOGIC — Step 7: write error file if there are rejections
        error_file_key: str | None = None
        if len(rejected_df) > 0:
            error_file_key = error_writer.write_error_file(
                rejected_df=rejected_df,
                bucket=bucket,
                desk_code=desk_code,
                trade_date_str=trade_date_str,
                timestamp_et=processing_timestamp_et,
            )
            logger.info("Error file written: s3_key=%s", error_file_key)

        # LOGIC — Step 8: build and write summary report
        report_s3_key, report_dict = report_builder.build_and_write_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            filename=filename,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            timestamp_et=processing_timestamp_et,
            bucket=bucket,
            error_file_key=error_file_key,
        )
        logger.info("Report written: s3_key=%s", report_s3_key)

        # LOGIC — Step 9: publish success notification
        notifier.notify_success(
            report_dict=report_dict,
            report_s3_key=report_s3_key,
        )
        logger.info("Success notification published")

        # LOGIC — Step 10: update audit record to SUCCESS
        db_loader.update_audit_record(
            conn=conn,
            audit_id=audit_id,
            status="SUCCESS",
            rows_inserted=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
        )
        logger.info("Audit record updated: audit_id=%s status=SUCCESS", audit_id)

        return {"statusCode": 200, "body": "OK"}

    except Exception as exc:  # LOGIC — catch-all for unhandled exceptions
        logger.exception(
            "Pipeline failed for file '%s': %s", filename or key, exc
        )

        # LOGIC — update audit record to FAILED if one was created
        if conn is not None and audit_id is not None:
            try:
                db_loader.update_audit_record(
                    conn=conn,
                    audit_id=audit_id,
                    status="FAILED",
                    rows_inserted=0,
                    rows_rejected=0,
                    error_message=str(exc),
                )
                logger.info(
                    "Audit record updated: audit_id=%s status=FAILED", audit_id
                )
            except Exception as audit_exc:
                logger.error(
                    "Failed to update audit record to FAILED: %s", audit_exc
                )

        # LOGIC — publish failure notification
        try:
            notifier.notify_failure(
                filename=filename or key,
                desk_code=desk_code,
                trade_date_str=trade_date_str,
                error_message=str(exc),
                processing_timestamp_et=processing_timestamp_et,
            )
            logger.info("Failure notification published")
        except Exception as notify_exc:
            logger.error("Failed to publish failure notification: %s", notify_exc)

        return {"statusCode": 500, "body": str(exc)}

    finally:
        # BOILERPLATE — always close the DB connection
        if conn is not None:
            try:
                conn.close()
                logger.info("DB connection closed")
            except Exception as close_exc:
                logger.error("Error closing DB connection: %s", close_exc)