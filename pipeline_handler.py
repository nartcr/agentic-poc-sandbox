# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime
from urllib.parse import unquote_plus

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — lazy imports of sibling modules (all must exist in deployment package)
from file_reader import read_position_file
from row_validator import validate_rows
from db_loader import load_positions
from report_builder import build_and_write_report
from audit_writer import write_audit_record
from sns_notifier import notify_success, notify_failure
from error_writer import write_error_file

# LOGIC — filename convention regex, per approved design
_FILENAME_RE = re.compile(r"^([A-Za-z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$")


def _parse_filename(key: str) -> tuple:
    """
    LOGIC: Extract desk_code and trade_date from the S3 object key.
    Operates on the basename only. Returns (desk_code, trade_date) strings.
    Raises ValueError if the basename does not match the expected pattern.
    """
    basename = key.split("/")[-1]
    match = _FILENAME_RE.match(basename)
    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match expected pattern "
            f"{{desk_code}}_{{YYYY-MM-DD}}_positions.csv"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return desk_code, trade_date


def handler(event: dict, context: object) -> dict:
    """
    LOGIC: AWS Lambda entry point. Orchestrates the full ingestion pipeline.
    Returns a structured JSON body with rows_inserted, rows_rejected,
    error_file, and report_file keys.
    """
    # LOGIC — capture ET timestamp as the very first action (TAC-7)
    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz)

    logger.info("Pipeline handler invoked at %s", processing_timestamp_et.isoformat())

    # BOILERPLATE — extract S3 event details
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        raw_key = record["s3"]["object"]["key"]
        key = unquote_plus(raw_key)  # LOGIC: S3 event keys may be URL-encoded
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event payload: %s", exc)
        return {"statusCode": 400, "body": json.dumps({"error": f"Malformed event: {exc}"})}

    logger.info("Processing S3 object: s3://%s/%s", bucket, key)

    # LOGIC — parse filename to extract desk_code and trade_date
    desk_code: str | None = None
    trade_date: str | None = None
    filename = key.split("/")[-1]

    try:
        desk_code, trade_date = _parse_filename(key)
    except ValueError as exc:
        logger.error("Filename validation failed: %s", exc)
        notify_failure(
            filename=filename,
            error=str(exc),
            desk_code=None,
            trade_date=None,
        )
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "error": str(exc),
                    "rows_inserted": 0,
                    "rows_rejected": 0,
                    "error_file": None,
                    "report_file": None,
                }
            ),
        }

    # LOGIC — variables that must be in scope for the finally/except audit block
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    error_file_key: str | None = None
    report_file_key: str | None = None
    report: dict = {}
    status = "FAILURE"
    error_message: str | None = None

    try:
        # LOGIC — Step 1: Read the CSV file from S3
        logger.info("Reading position file: s3://%s/%s", bucket, key)
        raw_df = read_position_file(bucket=bucket, key=key)
        total_rows = len(raw_df)
        logger.info("Raw rows read: %d", total_rows)

        # LOGIC — Step 2: Validate rows — split into valid and rejected sets
        logger.info("Validating rows")
        valid_df, rejected_df = validate_rows(raw_df)
        logger.info(
            "Validation complete: %d valid, %d rejected",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — Step 3: Write error file for rejected rows (if any)
        s3_bucket = os.environ["S3_BUCKET"]
        if len(rejected_df) > 0:
            logger.info("Writing error file for %d rejected rows", len(rejected_df))
            error_file_key = write_error_file(
                rejected_df=rejected_df,
                desk_code=desk_code,
                trade_date=trade_date,
                bucket=s3_bucket,
            )
            logger.info("Error file written: %s", error_file_key)
        else:
            logger.info("No rejected rows — skipping error file write")

        # LOGIC — Step 4: Load valid rows into Aurora PostgreSQL
        logger.info("Loading %d valid rows into database", len(valid_df))
        rows_inserted = load_positions(valid_df=valid_df)
        logger.info("Rows inserted: %d", rows_inserted)

        # LOGIC — Step 5: Build and write the summary report
        logger.info("Building summary report")
        report = build_and_write_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            rows_inserted=rows_inserted,
            processing_timestamp_et=processing_timestamp_et,
            bucket=s3_bucket,
        )
        report_file_key = report.get("_s3_report_key")  # LOGIC: populated by report_builder
        logger.info("Report written to S3")

        # LOGIC — Determine pipeline status for audit
        if len(rejected_df) == 0:
            status = "SUCCESS"
        else:
            status = "PARTIAL"

        # LOGIC — Step 6: Write audit record
        logger.info("Writing audit record with status: %s", status)
        write_audit_record(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
            processing_timestamp_et=processing_timestamp_et,
        )

        # LOGIC — Step 7: Publish SNS success notification
        logger.info("Publishing success notification")
        notify_success(report=report)

        logger.info(
            "Pipeline completed successfully: %d inserted, %d rejected",
            rows_inserted,
            len(rejected_df),
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": len(rejected_df),
                    "error_file": error_file_key,
                    "report_file": report_file_key,
                }
            ),
        }

    except Exception as exc:
        # LOGIC — top-level exception handler: audit, notify failure, re-raise
        error_message = str(exc)
        logger.exception("Unhandled exception in pipeline: %s", error_message)

        total_rows_for_audit = len(raw_df) if raw_df is not None else 0
        rows_rejected_for_audit = len(rejected_df) if rejected_df is not None else 0

        # LOGIC — attempt audit write; do not let audit failure mask original error
        try:
            write_audit_record(
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILURE",
                total_rows=total_rows_for_audit,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected_for_audit,
                error_message=error_message,
                processing_timestamp_et=processing_timestamp_et,
            )
        except Exception as audit_exc:
            logger.error("Failed to write audit record during failure handling: %s", audit_exc)

        # LOGIC — attempt failure notification; do not let SNS failure mask original error
        try:
            notify_failure(
                filename=filename,
                error=error_message,
                desk_code=desk_code,
                trade_date=trade_date,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": error_message,
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected_for_audit,
                    "error_file": error_file_key,
                    "report_file": report_file_key,
                }
            ),
        }