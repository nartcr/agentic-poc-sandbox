# BOILERPLATE
import logging
from datetime import datetime

import pytz

import audit
import config
import db
import error_writer
import file_reader
import loader
import notifier
import reporter
import secrets
import validator

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone used for all timestamps
_ET = pytz.timezone("America/Toronto")


# LOGIC
def run_pipeline(s3_key: str) -> None:
    """
    Orchestrates end-to-end processing of a single trade positions CSV file.
    Satisfies: BAC-1 through BAC-8 (entry point that invokes all pipeline stages).
    """
    # BOILERPLATE — Step 1: Load Config
    cfg = config.Config()

    conn = None
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    load_timestamp_et = None
    error_file_key = None
    report = None
    report_s3_key = None
    desk_code = None
    trade_date = None
    file_metadata = {}

    try:
        # LOGIC — Step 2: Retrieve DB credentials from Secrets Manager
        credentials = secrets.get_db_credentials()

        # LOGIC — Step 3: Open Aurora PostgreSQL connection
        conn = db.get_connection(credentials)

        # LOGIC — Step 4: Idempotent schema setup
        db.ensure_schema(conn)

        # LOGIC — Step 5: Download and parse CSV from S3
        raw_df, file_metadata = file_reader.read_csv_from_s3(cfg.S3_BUCKET, s3_key)
        desk_code = file_metadata["desk_code_from_filename"]
        trade_date = file_metadata["trade_date_from_filename"]

        logger.info(
            "File read: source_file=%s desk_code=%s trade_date=%s raw_rows=%d",
            s3_key,
            desk_code,
            trade_date,
            file_metadata.get("row_count_raw", len(raw_df)),
        )

        # LOGIC — Step 6: Validate rows; split into valid and rejected
        valid_df, rejected_df = validator.validate(raw_df, desk_code, trade_date)

        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — Step 7: Batch-insert valid rows into app.daily_trades
        rows_inserted = loader.load_trades(conn, valid_df, s3_key)

        # LOGIC — Step 8: Capture ET timestamp immediately after load
        load_timestamp_et = datetime.now(_ET)

        logger.info(
            "Load complete: rows_inserted=%d load_timestamp=%s",
            rows_inserted,
            load_timestamp_et.isoformat(),
        )

        # LOGIC — Step 9: Write error CSV to S3 (skipped if zero rejections)
        error_file_key = error_writer.write_error_file(
            bucket=cfg.S3_BUCKET,
            errors_prefix=cfg.S3_ERRORS_PREFIX,
            rejected_df=rejected_df,
            source_file_key=s3_key,
        )

        if error_file_key:
            logger.info("Error file written: %s", error_file_key)

        # LOGIC — Step 10: Build JSON summary report
        report = reporter.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            source_file=s3_key,
            load_timestamp_et=load_timestamp_et,
        )

        # LOGIC — Step 11: Write JSON report to S3
        report_s3_key = reporter.write_report(
            bucket=cfg.S3_BUCKET,
            reports_prefix=cfg.S3_REPORTS_PREFIX,
            report=report,
            source_file_key=s3_key,
        )

        logger.info("Report written: %s", report_s3_key)

        # LOGIC — attach report_s3_key and error_file_s3_key to report dict
        # so publish_success can include them in the SNS message
        report["report_s3_key"] = report_s3_key
        report["error_file_s3_key"] = error_file_key

        # LOGIC — Step 12: Publish success SNS notification
        message_id = notifier.publish_success(
            topic_arn=cfg.SNS_SUCCESS_TOPIC_ARN,
            report=report,
        )
        logger.info("Success SNS published: MessageId=%s", message_id)

        # LOGIC — Step 13: Write audit row (status=SUCCESS)
        processed_at = datetime.now(_ET)
        audit_row = {
            "source_file": s3_key,
            "desk_code": desk_code,
            "trade_date": _parse_trade_date(trade_date),
            "status": "SUCCESS",
            "rows_received": len(raw_df),
            "rows_loaded": rows_inserted,
            "rows_rejected": len(rejected_df),
            "error_message": None,
            "processed_at": processed_at,
            "report_s3_key": report_s3_key,
            "error_file_s3_key": error_file_key,
        }
        audit.record_audit(conn, audit_row)

        # LOGIC — Step 14: Commit transaction and close connection
        conn.commit()
        logger.info("Transaction committed for source_file=%s", s3_key)

    except Exception as exc:  # LOGIC — failure path
        error_type = type(exc).__name__
        error_detail = str(exc)

        logger.error(
            "Pipeline failure for source_file=%s: %s: %s",
            s3_key,
            error_type,
            error_detail,
            exc_info=True,
        )

        # LOGIC — publish failure SNS notification (best-effort)
        try:
            notifier.publish_failure(
                topic_arn=cfg.SNS_FAILURE_TOPIC_ARN,
                source_file=s3_key,
                error_type=error_type,
                error_detail=error_detail,
            )
        except Exception as sns_exc:
            logger.error(
                "Failed to publish failure SNS for source_file=%s: %s",
                s3_key,
                sns_exc,
            )

        # LOGIC — write failure audit row (best-effort; uses parsed metadata if available)
        if conn is not None:
            try:
                failure_processed_at = datetime.now(_ET)
                failure_audit_row = {
                    "source_file": s3_key,
                    "desk_code": desk_code or "",
                    "trade_date": _parse_trade_date(trade_date) if trade_date else None,
                    "status": "FAILURE",
                    "rows_received": len(raw_df) if raw_df is not None else 0,
                    "rows_loaded": rows_inserted,
                    "rows_rejected": len(rejected_df) if rejected_df is not None else 0,
                    "error_message": f"{error_type}: {error_detail}",
                    "processed_at": failure_processed_at,
                    "report_s3_key": report_s3_key,
                    "error_file_s3_key": error_file_key,
                }
                audit.record_audit(conn, failure_audit_row)
                conn.commit()
                logger.info(
                    "Failure audit record committed for source_file=%s", s3_key
                )
            except Exception as audit_exc:
                logger.error(
                    "Failed to write failure audit for source_file=%s: %s",
                    s3_key,
                    audit_exc,
                )
                # LOGIC — do not suppress the original exception
                try:
                    conn.rollback()
                except Exception:
                    pass

        # LOGIC — re-raise original exception after best-effort cleanup
        raise

    finally:
        # BOILERPLATE — always release the DB connection
        if conn is not None:
            try:
                conn.close()
                logger.info("DB connection closed for source_file=%s", s3_key)
            except Exception as close_exc:
                logger.warning(
                    "Error closing DB connection for source_file=%s: %s",
                    s3_key,
                    close_exc,
                )


# LOGIC — helper: parse trade_date string "YYYY-MM-DD" to datetime.date
def _parse_trade_date(trade_date_str: str):
    """
    Converts a trade_date string in YYYY-MM-DD format to a datetime.date object.
    Returns None if the string is None or unparseable (failure path guard).
    """
    if not trade_date_str:
        return None
    try:
        return datetime.strptime(trade_date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(
            "Could not parse trade_date_str=%s as YYYY-MM-DD", trade_date_str
        )
        return None