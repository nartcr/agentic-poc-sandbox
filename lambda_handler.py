# BOILERPLATE
import json
import logging
import os
import re

import pytz

# BOILERPLATE
import file_reader
import row_validator
import error_writer
import db_loader
import db_secrets
import report_builder
import report_writer
import sns_notifier
import audit_writer
from pipeline_exceptions import FileReadError, CredentialError, DatabaseError, ValidationError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact filename pattern from DATA CONTRACTS: {desk_code}_{trade_date}_positions.csv
# desk_code may itself contain underscores, but trade_date is always YYYY-MM-DD (fixed width).
# Pattern: everything up to the last occurrence of _YYYY-MM-DD_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _extract_filename_parts(key: str) -> tuple:
    """
    # LOGIC
    Extract desk_code and trade_date from the S3 object key.
    The key may include a path prefix (e.g. incoming/EQDESK_2026-06-01_positions.csv).
    Returns (desk_code, trade_date) or raises ValueError.
    """
    # LOGIC — strip any path prefix; only the basename is parsed
    basename = key.split("/")[-1]
    match = _FILENAME_PATTERN.match(basename)
    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match expected pattern "
            "'<desk_code>_<YYYY-MM-DD>_positions.csv'"
        )
    return match.group("desk_code"), match.group("trade_date")


def handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    Lambda entry point. Orchestrates the full trade position pipeline:
      1. Parse S3 event
      2. Read CSV from S3
      3. Validate rows
      4. Write error file for rejected rows
      5. Load valid rows into RDS
      6. Build summary report
      7. Write report and manifest to S3
      8. Publish SNS success notification
      9. Write audit record
    On any unhandled exception: publish SNS failure, write audit record, re-raise.
    """
    # BOILERPLATE — extract S3 event fields
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = record["object"]["key"]

    logger.info("Lambda triggered: bucket=%s key=%s", bucket, key)

    # LOGIC — parse filename for desk_code and trade_date
    desk_code: str | None = None
    trade_date: str | None = None
    source_filename: str = key.split("/")[-1]

    conn = None

    try:
        desk_code, trade_date = _extract_filename_parts(key)
        logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date)

        # BOILERPLATE — obtain DB connection once for the full pipeline
        conn = db_secrets.get_connection()

        # LOGIC — Step 1: read CSV from S3 into raw DataFrame (all columns as str)
        logger.info("Reading CSV from S3: s3://%s/%s", bucket, key)
        raw_df = file_reader.read_csv_from_s3(bucket, key)
        logger.info("CSV read: %d raw rows", len(raw_df))

        # LOGIC — Step 2: validate rows; split into valid and rejected sets
        logger.info("Validating rows")
        valid_df, rejected_df = row_validator.validate_dataframe(raw_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — Step 3: write rejected rows to S3 error file (may return None)
        error_key: str | None = None
        if not rejected_df.empty:
            logger.info("Writing error file for %d rejected rows", len(rejected_df))
            error_key = error_writer.write_error_file(rejected_df, source_filename, bucket)
            logger.info("Error file written: %s", error_key)
        else:
            logger.info("No rejected rows; skipping error file write")

        # LOGIC — Step 4: load valid rows into demo_schema.trade_positions
        logger.info("Loading %d valid rows into database", len(valid_df))
        rows_inserted = db_loader.load_positions(valid_df, conn)
        logger.info("Rows inserted (after deduplication): %d", rows_inserted)

        # LOGIC — commit the data load transaction
        conn.commit()
        logger.info("Data load transaction committed")

        # LOGIC — Step 5: build summary report dict
        logger.info("Building summary report")
        report = report_builder.build_report(
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            source_filename=source_filename,
            desk_code=desk_code,
            trade_date=trade_date,
        )

        # LOGIC — Step 6: write report JSON and manifest to S3
        logger.info("Writing report and manifest to S3")
        report_key = report_writer.write_report_to_s3(
            report=report,
            source_filename=source_filename,
            error_key=error_key,
            bucket=bucket,
        )
        logger.info("Report written: %s", report_key)

        # LOGIC — Step 7: publish SNS success notification
        logger.info("Publishing SNS success notification")
        sns_notifier.publish_success(report=report, report_s3_key=report_key)

        # LOGIC — Step 8: determine audit status
        # PARTIAL = some rows rejected but at least some were processed
        # SUCCESS = all rows valid and inserted (or file was empty with no rejections)
        if len(rejected_df) > 0 and rows_inserted >= 0:
            audit_status = "PARTIAL"
        else:
            audit_status = "SUCCESS"

        total_rows = len(valid_df) + len(rejected_df)

        # LOGIC — Step 9: write audit record (committed independently inside audit_writer)
        logger.info("Writing audit record: status=%s", audit_status)
        from datetime import datetime as _datetime  # BOILERPLATE — local import to avoid circular
        _et = pytz.timezone("America/Toronto")
        processing_ts = _datetime.now(_et)

        audit_writer.write_audit_record(
            conn=conn,
            filename=source_filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=audit_status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
            processing_timestamp_et=processing_ts,
        )

        logger.info("Pipeline completed successfully for file: %s", source_filename)

        # LOGIC — return JSON-parseable body with required keys
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": len(rejected_df),
                    "error_file": error_key,
                    "report_file": report_key,
                }
            ),
        }

    except Exception as exc:
        # LOGIC — failure path: notify, audit, re-raise
        logger.error(
            "Pipeline failed for file '%s': %s",
            source_filename,
            str(exc),
            exc_info=True,
        )

        # LOGIC — publish SNS failure notification
        try:
            sns_notifier.publish_failure(
                filename=source_filename,
                error_message=str(exc),
                desk_code=desk_code,
                trade_date=trade_date,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish SNS failure notification: %s", str(sns_exc))

        # LOGIC — write failure audit record (separate commit inside audit_writer)
        if conn is not None:
            try:
                # Roll back any uncommitted data-load changes before writing audit
                conn.rollback()
            except Exception as rb_exc:
                logger.error("Rollback failed: %s", str(rb_exc))

            try:
                from datetime import datetime as _datetime  # BOILERPLATE
                _et = pytz.timezone("America/Toronto")
                processing_ts = _datetime.now(_et)

                audit_writer.write_audit_record(
                    conn=conn,
                    filename=source_filename,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    status="FAILED",
                    total_rows=0,
                    rows_inserted=0,
                    rows_rejected=0,
                    error_message=str(exc),
                    processing_timestamp_et=processing_ts,
                )
            except Exception as audit_exc:
                logger.error("Failed to write failure audit record: %s", str(audit_exc))

        # LOGIC — close connection on failure path
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        raise

    finally:
        # BOILERPLATE — ensure connection is closed on success path too
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass