# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

# BOILERPLATE — project-local modules (all must exist per approved design)
from db_connection import get_connection
from db_loader import insert_positions, write_audit_record
from file_parser import read_position_file
from notification_publisher import publish_failure, publish_success
from report_builder import (
    build_report,
    write_errors_to_s3,
    write_manifest_to_s3,
    write_report_to_s3,
)
from row_validator import validate_rows
from timestamp_helper import format_et, now_et

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename convention: {desk_code}_{trade_date}_positions.csv
# desk_code may itself contain underscores, so we anchor on the date segment
# which is always YYYY-MM-DD followed by _positions.csv
_FILENAME_RE = re.compile(
    r"^(?P<desk_code>.+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _extract_s3_key(event: dict) -> tuple:
    # LOGIC — extract bucket and key from S3 event notification
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        logger.info("Extracted S3 event: bucket=%s key=%s", bucket, key)
        return bucket, key
    except (KeyError, IndexError) as exc:
        raise ValueError(
            f"Event does not contain a valid S3 record: {exc}"
        ) from exc


def _parse_filename(key: str) -> tuple:
    # LOGIC — parse desk_code and trade_date from the S3 object key basename
    # Uses regex to avoid ambiguity when desk_code contains underscores
    basename = os.path.basename(key)
    match = _FILENAME_RE.match(basename)
    if not match:
        raise ValueError(
            f"Filename '{basename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv' "
            f"where trade_date is YYYY-MM-DD"
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def _run_pipeline(bucket: str, key: str) -> dict:
    # LOGIC — orchestrate the full processing pipeline for one position file
    processing_ts: datetime = now_et()
    filename: str = os.path.basename(key)
    desk_code: str | None = None
    trade_date: str | None = None
    conn = None

    try:
        # Step 1 — parse filename to extract metadata
        desk_code, trade_date = _parse_filename(key)

        # Step 2 — read raw CSV from S3
        logger.info("Reading position file from s3://%s/%s", bucket, key)
        raw_df = read_position_file(bucket, key)
        total_rows = len(raw_df)
        logger.info("Read %d rows from file", total_rows)

        # Step 3 — validate rows, split into valid and rejected sets
        logger.info("Validating rows")
        valid_df, rejected_df = validate_rows(raw_df, desk_code, trade_date)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # Step 4 — open DB connection and insert valid rows
        logger.info("Opening database connection")
        conn = get_connection()
        rows_inserted = insert_positions(conn, valid_df)
        logger.info("Inserted %d rows into trade_positions", rows_inserted)

        # Step 5 — build summary report and write to S3
        logger.info("Building and writing report")
        report = build_report(
            valid_df=valid_df,
            rejected_df=rejected_df,
            raw_df=raw_df,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            rows_inserted=rows_inserted,
            processing_timestamp_et=processing_ts,
        )

        report_key = write_report_to_s3(
            report=report,
            bucket=bucket,
            desk_code=desk_code,
            trade_date=trade_date,
            timestamp_et=processing_ts,
        )
        logger.info("Report written to s3://%s/%s", bucket, report_key)

        # Step 6 — write error CSV if any rows were rejected
        error_key: str | None = None
        if len(rejected_df) > 0:
            error_key = write_errors_to_s3(
                rejected_df=rejected_df,
                bucket=bucket,
                desk_code=desk_code,
                trade_date=trade_date,
                timestamp_et=processing_ts,
            )
            logger.info("Error file written to s3://%s/%s", bucket, error_key)

        # Step 7 — write manifest
        manifest_key = write_manifest_to_s3(
            bucket=bucket,
            desk_code=desk_code,
            trade_date=trade_date,
            report_key=report_key,
            error_key=error_key,
            processing_timestamp_et=processing_ts,
        )
        logger.info("Manifest written to s3://%s/%s", bucket, manifest_key)

        # Step 8 — determine pipeline status for audit
        rows_rejected = len(rejected_df)
        if rows_rejected == 0:
            pipeline_status = "SUCCESS"
        elif rows_inserted == 0 and rows_rejected > 0:
            pipeline_status = "PARTIAL"
        else:
            pipeline_status = "PARTIAL" if rows_rejected > 0 else "SUCCESS"

        # Step 9 — write audit record
        write_audit_record(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=pipeline_status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
            processing_timestamp_et=processing_ts,
        )
        conn.commit()
        logger.info("Audit record written with status=%s", pipeline_status)

        # Step 10 — enrich report dict with keys needed for SNS and response
        report["report_s3_key"] = report_key
        report["manifest_s3_key"] = manifest_key

        # Step 11 — publish success notification
        publish_success(report)
        logger.info("Success notification published")

        # LOGIC — build summary dict returned to caller and serialised in response body
        summary = {
            "filename": filename,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "processing_timestamp_et": format_et(processing_ts),
            "total_rows": total_rows,
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "pipeline_status": pipeline_status,
            "report_s3_key": report_key,
            "manifest_s3_key": manifest_key,
            "error_file_key": error_key,
        }
        return summary

    except Exception:
        # LOGIC — on any unhandled exception, attempt to write a FAILURE audit record
        # then re-raise so the handler can build the error response and fire SNS failure
        if conn is not None:
            try:
                write_audit_record(
                    conn=conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    status="FAILURE",
                    total_rows=0,
                    rows_inserted=0,
                    rows_rejected=0,
                    error_message="Pipeline failed — see Lambda logs for details",
                    processing_timestamp_et=processing_ts,
                )
                conn.commit()
            except Exception as audit_exc:  # noqa: BLE001
                logger.error("Failed to write FAILURE audit record: %s", audit_exc)
        raise

    finally:
        if conn is not None:
            try:
                conn.close()
                logger.info("Database connection closed")
            except Exception as close_exc:  # noqa: BLE001
                logger.warning("Error closing database connection: %s", close_exc)


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — Lambda entry point
    logger.info("Lambda handler invoked")
    processing_ts: datetime = now_et()

    bucket: str | None = None
    key: str | None = None
    desk_code: str | None = None
    trade_date: str | None = None

    try:
        bucket, key = _extract_s3_key(event)
        filename = os.path.basename(key)

        # LOGIC — attempt filename parse early so failure notification has metadata
        try:
            desk_code, trade_date = _parse_filename(key)
        except ValueError:
            # filename is malformed — desk_code/trade_date stay None
            filename = os.path.basename(key) if key else "unknown"

        summary = _run_pipeline(bucket, key)
        logger.info("Pipeline completed successfully: %s", summary)

        return {
            "statusCode": 200,
            "body": json.dumps(summary),
        }

    except Exception as exc:  # noqa: BLE001
        # LOGIC — all unhandled pipeline failures route here
        error_str = str(exc)
        logger.error(
            "Pipeline failed for key=%s: %s",
            key,
            error_str,
            exc_info=True,
        )

        filename = os.path.basename(key) if key else "unknown"

        # LOGIC — fire failure SNS notification; swallow publish errors so Lambda
        # still returns a structured response rather than timing out
        try:
            publish_failure(
                filename=filename,
                error=error_str,
                desk_code=desk_code,
                trade_date=trade_date,
            )
            logger.info("Failure notification published")
        except Exception as notify_exc:  # noqa: BLE001
            logger.error("Failed to publish failure notification: %s", notify_exc)

        error_body = {
            "filename": filename,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "processing_timestamp_et": format_et(processing_ts),
            "error": error_str,
            "rows_inserted": 0,
            "rows_rejected": 0,
            "error_file_key": None,
            "report_s3_key": None,
        }
        return {
            "statusCode": 500,
            "body": json.dumps(error_body),
        }