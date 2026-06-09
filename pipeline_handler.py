# BOILERPLATE
import json
import logging
import os
import re

import boto3

import file_ingestor
import row_validator
import db_loader
import error_writer
import report_writer
import sns_notifier
import secret_manager
import time_utils

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern: incoming/{desk_code}_{YYYY-MM-DD}_positions.csv
# The trade_date is the anchor: exactly YYYY-MM-DD (digits only, fixed width).
# desk_code may itself contain underscores, so we anchor on the date pattern.
_FILENAME_RE = re.compile(
    r"^incoming/(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def parse_s3_event(event: dict) -> tuple:
    """
    # LOGIC
    Extract bucket name and object key from an S3 event notification payload.
    Returns (bucket, key).
    Raises KeyError if the expected structure is absent.
    """
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    logger.info("Parsed S3 event: bucket=%s key=%s", bucket, key)
    return bucket, key


def extract_filename_parts(key: str) -> tuple:
    """
    # LOGIC
    Validate that the S3 object key matches the expected pattern
    incoming/{desk_code}_{trade_date}_positions.csv and return
    (desk_code, trade_date_str).
    Raises ValueError if the key does not match.
    """
    match = _FILENAME_RE.match(key)
    if not match:
        raise ValueError(
            f"Filename key '{key}' does not match expected pattern "
            "'incoming/{{desk_code}}_{{YYYY-MM-DD}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date_str = match.group("trade_date")
    logger.info("Extracted filename parts: desk_code=%s trade_date=%s", desk_code, trade_date_str)
    return desk_code, trade_date_str


def handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    Lambda entry point. Orchestrates the full file-processing pipeline:
      1. Parse S3 event
      2. Validate filename pattern
      3. Ingest file from S3
      4. Validate rows
      5. Load valid rows to DB
      6. Write error file (if rejections exist)
      7. Write summary report
      8. Write audit record
      9. Publish SNS notification

    Returns HTTP-style dict with statusCode 200 on success/partial,
    500 on unhandled infrastructure failure.
    """
    # BOILERPLATE — initialise shared clients once per invocation
    now = time_utils.now_et()
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # BOILERPLATE — read environment variables
    s3_bucket = os.environ["S3_BUCKET"]
    sns_success_arn = os.environ["SNS_SUCCESS_ARN"]
    sns_failure_arn = os.environ["SNS_FAILURE_ARN"]

    # LOGIC — mutable state collected during pipeline execution
    filename = "UNKNOWN"
    desk_code = None
    trade_date_str = None
    total_rows = 0
    rows_inserted = 0
    rows_rejected = 0
    error_file_key = None
    report_file_key = None
    conn = None

    try:
        # LOGIC — Step 1: parse S3 event
        bucket, key = parse_s3_event(event)
        filename = key.split("/")[-1]

        # LOGIC — Step 2: validate filename pattern
        desk_code, trade_date_str = extract_filename_parts(key)

        # LOGIC — Step 3: obtain DB connection
        conn = secret_manager.get_db_connection()

        # LOGIC — Step 4: ingest file from S3
        raw_df, total_rows = file_ingestor.read_position_file(bucket, key, s3_client)
        logger.info("Ingested %d rows from s3://%s/%s", total_rows, bucket, key)

        # LOGIC — Step 5: validate rows
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d", len(valid_df), rows_rejected
        )

        # LOGIC — Step 6: load valid rows to DB
        rows_inserted = 0
        if not valid_df.empty:
            rows_inserted = db_loader.load_positions(valid_df, conn)
        logger.info("Loaded %d rows into trade_positions", rows_inserted)

        # LOGIC — Step 7: write error file if there are rejected rows
        if rows_rejected > 0:
            error_file_key = error_writer.write_error_file(
                rejected_df=rejected_df,
                bucket=s3_bucket,
                desk_code=desk_code,
                trade_date_str=trade_date_str,
                s3_client=s3_client,
                now_et=now,
            )
            error_writer.write_error_manifest(
                bucket=s3_bucket,
                desk_code=desk_code,
                trade_date_str=trade_date_str,
                error_key=error_file_key,
                row_count=rows_rejected,
                s3_client=s3_client,
                now_et=now,
            )
            logger.info("Error file written: %s", error_file_key)

        # LOGIC — Step 8: build and write summary report
        report = report_writer.build_report(
            filename=filename,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            now_et=now,
        )
        report_file_key = report_writer.write_report(
            report=report,
            bucket=s3_bucket,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            s3_client=s3_client,
            now_et=now,
        )
        report_writer.write_report_manifest(
            bucket=s3_bucket,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            report_key=report_file_key,
            s3_client=s3_client,
            now_et=now,
        )
        logger.info("Report written: %s", report_file_key)

        # LOGIC — Step 9: determine audit status
        if rows_rejected > 0 and rows_inserted > 0:
            status = "PARTIAL"
        elif rows_rejected > 0 and rows_inserted == 0:
            status = "PARTIAL"
        else:
            status = "SUCCESS"

        # LOGIC — Step 10: write audit record
        from datetime import date as _date
        import datetime as _dt

        # Parse trade_date for the audit record
        try:
            audit_trade_date = _dt.datetime.strptime(trade_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            audit_trade_date = None

        db_loader.write_audit_record(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=audit_trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
            processing_timestamp_et=now,
        )

        # LOGIC — Step 11: publish success SNS notification
        sns_notifier.notify_success(
            sns_client=sns_client,
            topic_arn=sns_success_arn,
            filename=filename,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            report_key=report_file_key,
            processing_timestamp_et=now,
        )

        # LOGIC — Step 12: return success response
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file": error_file_key,
            "report_file": report_file_key,
        }
        logger.info("Pipeline completed successfully: %s", response_body)
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:  # LOGIC — catch-all for unhandled failures
        error_message = str(exc)
        logger.exception(
            "Pipeline failed for file '%s': %s", filename, error_message
        )

        # LOGIC — attempt to write failure audit record; do not swallow secondary errors
        if conn is not None:
            try:
                import datetime as _dt

                audit_trade_date = None
                if trade_date_str:
                    try:
                        audit_trade_date = _dt.datetime.strptime(
                            trade_date_str, "%Y-%m-%d"
                        ).date()
                    except ValueError:
                        audit_trade_date = None

                db_loader.write_audit_record(
                    conn=conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date=audit_trade_date,
                    status="FAILED",
                    total_rows=total_rows,
                    rows_inserted=rows_inserted,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                    processing_timestamp_et=now,
                )
            except Exception as audit_exc:
                logger.error(
                    "Failed to write audit record for failure: %s", str(audit_exc)
                )

        # LOGIC — attempt to publish failure SNS notification; do not swallow secondary errors
        try:
            sns_notifier.notify_failure(
                sns_client=sns_client,
                topic_arn=sns_failure_arn,
                filename=filename,
                error_message=error_message,
                processing_timestamp_et=now,
            )
        except Exception as sns_exc:
            logger.error(
                "Failed to publish failure SNS notification: %s", str(sns_exc)
            )

        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file": error_file_key,
            "report_file": report_file_key,
        }
        return {"statusCode": 500, "body": json.dumps(response_body)}