# BOILERPLATE
import json
import logging
import os

import psycopg2

import audit_logger
import db_connection
import db_loader
import error_writer
import file_reader
import report_builder
import row_validator
import sns_notifier
import time_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def _parse_s3_event(event: dict) -> tuple:
    """Extract (bucket, key) from a standard S3 event notification."""
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as exc:
        raise ValueError(f"Malformed S3 event payload: {exc}") from exc
    return bucket, key


# LOGIC
def _run_pipeline(bucket: str, key: str) -> dict:
    """
    Orchestrate the full pipeline for one file:
      download → validate → db load → error file → report → SNS success
    Returns the summary dict produced by report_builder.
    """
    # LOGIC — download and parse
    raw_df, metadata = file_reader.download_and_parse(bucket, key)
    desk_code = metadata["desk_code"]
    trade_date = metadata["trade_date"]

    logger.info(
        "File downloaded. desk_code=%s trade_date=%s rows=%d",
        desk_code,
        trade_date,
        len(raw_df),
    )

    # LOGIC — validate
    valid_df, rejected_df = row_validator.validate(raw_df)
    logger.info(
        "Validation complete. valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    # LOGIC — write rejection file (no-op if empty)
    error_prefix = os.environ["S3_ERROR_PREFIX"]
    error_writer.write_error_file(
        rejected_df,
        desk_code=desk_code,
        trade_date=trade_date,
        bucket=bucket,
        error_prefix=error_prefix,
    )

    # LOGIC — database load (connection opened here, closed in handler finally)
    secret_id = os.environ["DB_SECRET_ID"]
    conn = db_connection.get_connection(secret_id)

    try:
        rows_inserted = db_loader.load_positions(valid_df, conn)
        logger.info("DB load complete. rows_inserted=%d", rows_inserted)
    except Exception:
        db_connection.close_connection(conn)
        raise

    # LOGIC — build and publish report
    processing_timestamp_et = time_utils.now_et()
    report_prefix = os.environ["S3_REPORT_PREFIX"]
    summary = report_builder.build_and_publish_report(
        raw_df=raw_df,
        valid_df=valid_df,
        rejected_df=rejected_df,
        rows_inserted=rows_inserted,
        desk_code=desk_code,
        trade_date=trade_date,
        processing_timestamp_et=processing_timestamp_et,
        bucket=bucket,
        report_prefix=report_prefix,
    )

    # LOGIC — publish success notification
    sns_notifier.notify_success(summary)
    logger.info("Success notification published.")

    # LOGIC — write audit record (SUCCESS)
    audit_logger.write_audit_record(
        conn=conn,
        filename=metadata["filename"],
        desk_code=desk_code,
        trade_date=trade_date,
        status="SUCCESS",
        total_rows=len(raw_df),
        rows_inserted=rows_inserted,
        rows_rejected=len(rejected_df),
        error_message=None,
        processing_timestamp_et=processing_timestamp_et,
    )

    db_connection.close_connection(conn)
    return summary


# LOGIC
def handler(event: dict, context: object) -> dict:
    """
    Lambda entry point.

    Parses the S3 event, runs the full pipeline, and writes one audit row
    regardless of outcome. On failure, publishes to the failure SNS topic
    and writes a FAILURE audit row.
    """
    processing_timestamp_et = time_utils.now_et()
    bucket = None
    key = None
    filename = "UNKNOWN"
    desk_code = None
    trade_date = None
    conn = None

    try:
        # LOGIC — extract event coordinates
        bucket, key = _parse_s3_event(event)
        filename = key.split("/")[-1]  # bare filename for audit / SNS
        logger.info("Invocation started. bucket=%s key=%s", bucket, key)

        summary = _run_pipeline(bucket, key)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Pipeline completed successfully.",
                    "filename": filename,
                    "rows_inserted": summary.get("rows_inserted"),
                    "rows_rejected": summary.get("rows_rejected"),
                }
            ),
        }

    except Exception as exc:  # LOGIC — failure path
        error_type = type(exc).__name__
        error_message = str(exc)
        logger.exception(
            "Pipeline FAILURE. filename=%s error_type=%s error=%s",
            filename,
            error_type,
            error_message,
        )

        # LOGIC — publish failure notification
        try:
            sns_notifier.notify_failure(
                filename=filename,
                error_type=error_type,
                error_message=error_message,
                processing_timestamp_et=time_utils.to_et_isoformat(
                    processing_timestamp_et
                ),
            )
        except Exception as sns_exc:  # BOILERPLATE — swallow SNS errors to preserve audit write
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        # LOGIC — write FAILURE audit record
        try:
            secret_id = os.environ.get("DB_SECRET_ID")
            if secret_id and conn is None:
                conn = db_connection.get_connection(secret_id)
        except Exception as conn_exc:
            logger.error(
                "Could not open DB connection for failure audit write: %s", conn_exc
            )

        if conn is not None:
            try:
                # LOGIC — attempt to extract desk_code / trade_date from filename if parsing succeeded
                if desk_code is None and filename != "UNKNOWN":
                    try:
                        meta = file_reader._extract_filename_metadata(
                            filename
                        )
                        desk_code = meta.get("desk_code")
                        trade_date = meta.get("trade_date")
                    except Exception:
                        pass  # filename parse already failed; leave as None

                audit_logger.write_audit_record(
                    conn=conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    status="FAILURE",
                    total_rows=0,
                    rows_inserted=0,
                    rows_rejected=0,
                    error_message=error_message,
                    processing_timestamp_et=processing_timestamp_et,
                )
            except Exception as audit_exc:
                logger.error("Failed to write failure audit record: %s", audit_exc)
            finally:
                db_connection.close_connection(conn)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "Pipeline failed.",
                    "filename": filename,
                    "error_type": error_type,
                    "error_message": error_message,
                }
            ),
        }