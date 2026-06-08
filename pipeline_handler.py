# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

import audit_writer
import db_loader
import error_writer
import file_reader
import report_builder
import row_validator
import sns_notifier
from pipeline_exceptions import FilenameParseError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")

# LOGIC — regex pattern for the exact filename convention specified in the data contracts
# Pattern: incoming/{desk_code}_{trade_date}_positions.csv
# trade_date is YYYY-MM-DD; desk_code may contain letters, digits, hyphens, dots but NOT underscores
# (if desk_code itself can contain underscores the regex uses a non-greedy match up to the date segment)
_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>.+?)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_s3_key(key: str) -> tuple:
    """
    # LOGIC
    Parse desk_code and trade_date from the S3 object key.
    Raises FilenameParseError if the key does not match the expected pattern.
    """
    match = _KEY_PATTERN.match(key)
    if not match:
        raise FilenameParseError(
            f"S3 key '{key}' does not match expected pattern "
            "'incoming/{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    logger.info("Parsed filename — desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def _determine_status(rows_inserted: int, rows_rejected: int) -> str:
    """
    # LOGIC
    Determine pipeline audit status based on outcome counts.
    SUCCESS  — all rows valid and inserted (or all duplicates, zero rejections)
    PARTIAL  — some rows inserted, some rejected
    """
    if rows_rejected == 0:
        return "SUCCESS"
    if rows_inserted > 0 or rows_rejected > 0:
        return "PARTIAL"
    return "PARTIAL"


def lambda_handler(event: dict, context) -> dict:
    """
    # LOGIC
    AWS Lambda entry point. Receives an S3 event notification, orchestrates the
    full trade-position ingestion pipeline, and returns a structured response.
    """
    # BOILERPLATE — capture processing timestamp once, ET-localised
    processing_ts: datetime = datetime.now(_ET)
    logger.info("Lambda invoked at %s ET", processing_ts.isoformat())

    # BOILERPLATE — read environment
    bucket: str = os.environ["S3_BUCKET"]

    # LOGIC — extract S3 event coordinates
    s3_record = event["Records"][0]["s3"]
    raw_key: str = s3_record["object"]["key"]
    # S3 event keys may be URL-encoded; decode common encoding
    from urllib.parse import unquote_plus
    key: str = unquote_plus(raw_key)
    logger.info("Processing s3://%s/%s", bucket, key)

    # These are set early so the except block can reference them even if parsing fails
    desk_code = None
    trade_date = None
    report = {}
    error_file_key = None
    report_file_key = None
    rows_inserted = 0
    rows_rejected = 0
    total_rows = 0

    try:
        # LOGIC — parse filename to extract desk_code and trade_date
        desk_code, trade_date = _parse_s3_key(key)

        # LOGIC — step 1: read CSV from S3
        logger.info("Reading CSV from S3")
        raw_df = file_reader.read_csv_from_s3(bucket, key)
        total_rows = len(raw_df)
        logger.info("Read %d rows from input file", total_rows)

        # LOGIC — step 2: validate rows
        logger.info("Validating rows")
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete — valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — write error file if there are rejected rows
        error_file_key = None
        if rows_rejected > 0:
            logger.info("Writing error file for %d rejected rows", rows_rejected)
            error_file_key = error_writer.write_error_file(
                rejected_df, bucket, desk_code, trade_date, processing_ts
            )
            logger.info("Error file written to %s", error_file_key)

        # LOGIC — step 3: load valid rows into the database
        logger.info("Loading %d valid rows into database", len(valid_df))
        rows_inserted = db_loader.load_positions(valid_df)
        logger.info("Loaded %d rows (duplicates skipped via ON CONFLICT)", rows_inserted)

        # LOGIC — step 4: build summary report
        logger.info("Building summary report")
        report = report_builder.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_ts=processing_ts,
        )
        # Attach the error file key so the report and manifest are consistent
        report["error_file_key"] = error_file_key

        # LOGIC — write the report and manifest JSON files to S3
        report_file_key = report_builder.write_report(
            report=report,
            bucket=bucket,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_ts=processing_ts,
        )
        logger.info("Report written to %s", report_file_key)

        # LOGIC — determine status for audit record
        status = _determine_status(rows_inserted, rows_rejected)
        logger.info("Pipeline status: %s", status)

        # LOGIC — step 5: write audit record
        from datetime import date as date_type
        audit_trade_date = (
            date_type.fromisoformat(trade_date) if trade_date else None
        )
        error_message = None
        if rows_rejected > 0:
            error_message = (
                f"{rows_rejected} row(s) rejected; see error file: {error_file_key}"
            )

        audit_writer.write_audit_record(
            filename=key,
            desk_code=desk_code,
            trade_date=audit_trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=error_message,
            processing_ts=processing_ts,
        )
        logger.info("Audit record written with status=%s", status)

        # LOGIC — step 6: notify success via SNS
        sns_notifier.notify_success(report)
        logger.info("Success notification published")

        # LOGIC — build and return Lambda response
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file": error_file_key,
            "report_file": report_file_key,
        }
        logger.info("Lambda returning 200: %s", response_body)
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:  # LOGIC — top-level failure handler
        logger.exception("Unhandled exception during pipeline execution: %s", exc)

        # LOGIC — attempt to write failure audit record (best-effort)
        try:
            from datetime import date as date_type
            audit_trade_date = (
                date_type.fromisoformat(trade_date)
                if trade_date and isinstance(trade_date, str)
                else None
            )
            audit_writer.write_audit_record(
                filename=key,
                desk_code=desk_code,
                trade_date=audit_trade_date,
                status="FAILED",
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=str(exc),
                processing_ts=processing_ts,
            )
            logger.info("Failure audit record written")
        except Exception as audit_exc:
            logger.error(
                "Could not write failure audit record: %s", audit_exc
            )

        # LOGIC — attempt to publish failure SNS notification (best-effort)
        try:
            sns_notifier.notify_failure(
                filename=key,
                error_message=str(exc),
                desk_code=desk_code,
                trade_date=trade_date,
                processing_ts=processing_ts,
            )
            logger.info("Failure notification published")
        except Exception as sns_exc:
            logger.error(
                "Could not publish failure SNS notification: %s", sns_exc
            )

        # LOGIC — re-raise so Lambda marks the invocation as failed
        raise