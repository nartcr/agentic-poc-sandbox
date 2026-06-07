# BOILERPLATE
import logging
import os

import pytz

from datetime import datetime

# BOILERPLATE — submodule imports (all modules expected in same deployment package)
import db_connection
import file_reader
import row_validator
import db_loader
import error_writer
import report_builder
import audit_writer
import notification_publisher

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — mandatory filename suffix token
_POSITIONS_SUFFIX = "positions"


def _parse_filename(object_key: str) -> tuple:
    """
    # LOGIC
    Strips the incoming/ prefix and .csv suffix from the S3 object key,
    then splits on '_' to extract desk_code and trade_date.

    Returns (filename, desk_code, trade_date) on success.
    Raises ValueError with a descriptive message if the pattern does not match.
    """
    # LOGIC — strip prefix
    prefix = "incoming/"
    if object_key.startswith(prefix):
        bare = object_key[len(prefix):]
    else:
        bare = object_key

    # LOGIC — strip .csv suffix
    if not bare.endswith(".csv"):
        raise ValueError(
            f"Filename '{bare}' does not end with .csv"
        )
    stem = bare[: -len(".csv")]

    # LOGIC — split and validate parts: {desk_code}_{trade_date}_positions
    parts = stem.split("_")
    # trade_date is YYYY-MM-DD which contains two underscores when split,
    # so parts must be exactly 5 tokens: desk_code, YYYY, MM, DD, positions
    # Reconstruct trade_date from parts[1], parts[2], parts[3]
    if len(parts) != 5:
        raise ValueError(
            f"Filename stem '{stem}' does not match pattern "
            f"{{desk_code}}_{{YYYY}}-{{MM}}-{{DD}}_positions; got {len(parts)} parts"
        )

    desk_code = parts[0]
    trade_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
    suffix_token = parts[4]

    if suffix_token != _POSITIONS_SUFFIX:
        raise ValueError(
            f"Filename stem '{stem}' does not end with '_positions'; "
            f"got suffix '{suffix_token}'"
        )

    # LOGIC — validate desk_code is non-empty
    if not desk_code:
        raise ValueError("desk_code parsed from filename is empty")

    # LOGIC — validate trade_date is parseable as YYYY-MM-DD
    try:
        datetime.strptime(trade_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"trade_date '{trade_date}' parsed from filename is not a valid YYYY-MM-DD date"
        )

    return bare, desk_code, trade_date


def lambda_handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    AWS Lambda entry point. Triggered by S3 ObjectCreated event on the
    incoming/ prefix. Orchestrates the full position ingestion pipeline.

    Returns {"statusCode": 200, "body": "OK"} on success.
    Re-raises on unrecoverable failure after writing FAILED audit record
    and publishing failure SNS notification.
    """
    # BOILERPLATE — extract S3 event metadata
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    logger.info(
        "Lambda invoked: bucket=%s key=%s", bucket, object_key
    )

    # LOGIC — derive filename (basename only, for audit/reporting)
    filename = object_key.split("/")[-1]

    # LOGIC — parse filename; on failure write FAILED audit and publish failure notification
    desk_code = None
    trade_date = None
    try:
        filename_bare, desk_code, trade_date = _parse_filename(object_key)
        filename = filename_bare  # use the stripped filename for audit records
    except ValueError as parse_err:
        logger.error(
            "Filename parse failed for key '%s': %s", object_key, parse_err
        )
        _handle_failure(
            filename=filename,
            desk_code=None,
            trade_date=None,
            error_message=str(parse_err),
        )
        raise

    # LOGIC — main pipeline; any unhandled exception triggers failure path
    conn = None
    try:
        # LOGIC — open DB connection once for the entire invocation
        conn = db_connection.get_connection()

        # LOGIC — step 1: read raw CSV from S3
        raw_df = file_reader.read_position_file(bucket, object_key)
        logger.info(
            "File read: key=%s rows=%d", object_key, len(raw_df)
        )

        # LOGIC — step 2: validate rows
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 3: load valid rows into DB
        rows_inserted = db_loader.load_positions(valid_df, conn)
        logger.info(
            "DB load complete: inserted=%d skipped=%d",
            rows_inserted,
            len(valid_df) - rows_inserted,
        )

        # LOGIC — step 4: write error file for rejected rows (no-op if empty)
        error_s3_key = error_writer.write_error_file(
            rejected_df, bucket, desk_code, trade_date
        )
        if error_s3_key:
            logger.info("Error file written: s3://%s/%s", bucket, error_s3_key)

        # LOGIC — step 5: build and write summary report to S3
        summary = report_builder.build_and_write_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            bucket=bucket,
            desk_code=desk_code,
            trade_date=trade_date,
        )
        logger.info(
            "Report written: s3://%s/reports/%s_%s_positions_report.json",
            bucket,
            desk_code,
            trade_date,
        )

        # LOGIC — determine audit status
        rows_rejected_count = len(rejected_df)
        if rows_rejected_count > 0 and rows_inserted > 0:
            status = "PARTIAL"
        elif rows_rejected_count > 0 and rows_inserted == 0:
            # All rows were either rejected or skipped duplicates — treat as PARTIAL
            # if any valid rows existed (even if all were duplicates), else FAILED
            if len(valid_df) > 0:
                status = "PARTIAL"
            else:
                status = "FAILED"
        else:
            status = "SUCCESS"

        # LOGIC — step 6: write audit record
        audit_writer.write_audit_record(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=len(raw_df),
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected_count,
            error_message=None,
        )
        logger.info("Audit record written: status=%s", status)

        # LOGIC — step 7: publish success SNS notification
        notification_publisher.publish_success(summary)
        logger.info(
            "Success notification published: desk_code=%s trade_date=%s",
            desk_code,
            trade_date,
        )

    except Exception as pipeline_err:
        logger.exception(
            "Pipeline failed for key '%s': %s", object_key, pipeline_err
        )
        _handle_failure(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            error_message=str(pipeline_err),
            conn=conn,
        )
        raise

    finally:
        # BOILERPLATE — always close the DB connection if it was opened
        if conn is not None:
            try:
                conn.close()
                logger.info("DB connection closed")
            except Exception:
                logger.warning("Failed to close DB connection", exc_info=True)

    return {"statusCode": 200, "body": "OK"}


def _handle_failure(
    filename: str,
    desk_code,
    trade_date,
    error_message: str,
    conn=None,
) -> None:
    """
    # LOGIC
    Writes a FAILED audit record and publishes a failure SNS notification.
    Exceptions raised during these operations are caught and logged so they
    do not mask the original pipeline error.
    """
    # LOGIC — attempt to write FAILED audit record
    audit_conn = conn
    audit_conn_opened_here = False

    try:
        if audit_conn is None:
            # LOGIC — attempt to open a fresh connection just for the audit write
            try:
                audit_conn = db_connection.get_connection()
                audit_conn_opened_here = True
            except Exception as conn_err:
                logger.error(
                    "Cannot open DB connection for FAILED audit write: %s", conn_err
                )
                audit_conn = None

        if audit_conn is not None:
            audit_writer.write_audit_record(
                conn=audit_conn,
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=0,
                rows_inserted=0,
                rows_rejected=0,
                error_message=error_message,
            )
            logger.info("FAILED audit record written for filename='%s'", filename)
    except Exception as audit_err:
        logger.error(
            "Failed to write FAILED audit record for '%s': %s",
            filename,
            audit_err,
        )
    finally:
        if audit_conn_opened_here and audit_conn is not None:
            try:
                audit_conn.close()
            except Exception:
                pass

    # LOGIC — publish failure SNS notification
    try:
        notification_publisher.publish_failure(
            filename=filename,
            error_message=error_message,
            desk_code=desk_code,
            trade_date=trade_date,
        )
        logger.info(
            "Failure notification published for filename='%s'", filename
        )
    except Exception as sns_err:
        logger.error(
            "Failed to publish failure notification for '%s': %s",
            filename,
            sns_err,
        )