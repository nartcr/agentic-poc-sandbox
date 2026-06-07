import logging
import os
import boto3
import pytz
from datetime import datetime

# BOILERPLATE — sibling module imports
import file_parser
import row_validator
import db_loader
import error_writer
import report_writer
import audit_writer
import sns_notifier
import db_connection

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — timezone constant
_ET = pytz.timezone("America/Toronto")


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — AWS client instantiation (once per invocation)
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")
    sm_client = boto3.client("secretsmanager")

    # BOILERPLATE — environment variable reads
    bucket = os.environ["S3_BUCKET"]
    sns_success_arn = os.environ["SNS_SUCCESS_ARN"]
    sns_failure_arn = os.environ["SNS_FAILURE_ARN"]

    # LOGIC — extract S3 key from the incoming event
    try:
        s3_key = _extract_s3_key(event)
    except (KeyError, IndexError, ValueError) as exc:
        logger.error("Failed to extract S3 key from event: %s", exc)
        sns_notifier.notify_failure(
            sns_client,
            sns_failure_arn,
            filename="UNKNOWN",
            error=str(exc),
            desk_code=None,
            trade_date_str=None,
        )
        return {"statusCode": 500, "body": {"error": str(exc), "filename": "UNKNOWN"}}

    filename = s3_key.split("/")[-1]
    logger.info("Starting pipeline for key=%s filename=%s", s3_key, filename)

    # LOGIC — run the full pipeline; catch all unhandled exceptions
    try:
        summary = _run_pipeline(
            s3_key=s3_key,
            bucket=bucket,
            s3_client=s3_client,
            sns_client=sns_client,
            sm_client=sm_client,
            sns_success_arn=sns_success_arn,
            sns_failure_arn=sns_failure_arn,
        )
        logger.info("Pipeline completed successfully for key=%s summary=%s", s3_key, summary)
        return {"statusCode": 200, "body": summary}

    except Exception as exc:  # LOGIC — catch-all: route to failure SNS
        logger.exception("Unhandled pipeline failure for key=%s: %s", s3_key, exc)
        sns_notifier.notify_failure(
            sns_client,
            sns_failure_arn,
            filename=filename,
            error=str(exc),
            desk_code=None,
            trade_date_str=None,
        )
        return {"statusCode": 500, "body": {"error": str(exc), "filename": filename}}


def _extract_s3_key(event: dict) -> str:
    # LOGIC — support both S3 event notification and direct invocation payloads
    if "Records" in event:
        # Standard S3 event notification structure
        record = event["Records"][0]
        bucket_from_event = record["s3"]["bucket"]["name"]  # noqa: F841 — available for logging
        key = record["s3"]["object"]["key"]
        logger.info("Extracted S3 key from Records event: %s", key)
        return key
    elif "s3_key" in event:
        # Direct invocation payload
        key = event["s3_key"]
        logger.info("Extracted S3 key from direct invocation: %s", key)
        return key
    else:
        raise ValueError(
            "Event does not contain 'Records' (S3 notification) or 's3_key' (direct invocation). "
            f"Top-level keys present: {list(event.keys())}"
        )


def _run_pipeline(
    s3_key: str,
    bucket: str,
    s3_client,
    sns_client,
    sm_client,
    sns_success_arn: str,
    sns_failure_arn: str,
) -> dict:
    # BOILERPLATE — capture processing timestamp once, ET, for all downstream writers
    processing_ts_et: datetime = datetime.now(_ET)

    filename = s3_key.split("/")[-1]

    # LOGIC — pipeline state accumulators (defaults for audit on early failure)
    desk_code: str | None = None
    trade_date_str: str | None = None
    total_rows: int = 0
    rows_inserted: int = 0
    rows_rejected: int = 0
    valid_rows: list = []
    rejected_rows: list = []
    error_file_key: str = ""
    report_key: str = ""
    report_dict: dict = {}
    db_conn = None

    try:
        # ── STEP 1: Open DB connection ──────────────────────────────────────
        db_conn = db_connection.get_connection(sm_client)
        logger.info("DB connection established")

        # ── STEP 2: Parse S3 file ───────────────────────────────────────────
        rows, desk_code, trade_date_str = file_parser.parse_s3_file(s3_client, bucket, s3_key)
        total_rows = len(rows)
        logger.info(
            "Parsed %d rows from %s (desk_code=%s trade_date=%s)",
            total_rows, filename, desk_code, trade_date_str,
        )

        # ── STEP 3: Validate rows ───────────────────────────────────────────
        valid_rows, rejected_rows = row_validator.validate_rows(
            rows, desk_code, trade_date_str
        )
        rows_rejected = len(rejected_rows)
        logger.info(
            "Validation complete: valid=%d rejected=%d", len(valid_rows), rows_rejected
        )

        # ── STEP 4: Load to DB ──────────────────────────────────────────────
        rows_inserted = db_loader.load_positions(valid_rows, db_conn)
        db_conn.commit()
        logger.info("Loaded %d rows into DB (valid=%d)", rows_inserted, len(valid_rows))

        # ── STEP 5: Write error file ────────────────────────────────────────
        error_file_key = error_writer.write_error_file(
            s3_client, bucket, rejected_rows, desk_code, trade_date_str, processing_ts_et
        )
        logger.info("Error file written to %s", error_file_key)

        # ── STEP 6: Write summary report ────────────────────────────────────
        report_key, report_dict = report_writer.write_report(
            s3_client,
            bucket,
            valid_rows,
            rejected_rows,
            desk_code,
            trade_date_str,
            rows_inserted,
            processing_ts_et,
        )
        logger.info("Report written to %s", report_key)

        # ── STEP 7: Write audit record (SUCCESS / PARTIAL) ──────────────────
        # LOGIC — PARTIAL if any rows were rejected; SUCCESS if all rows loaded cleanly
        status = "PARTIAL" if rows_rejected > 0 else "SUCCESS"
        import datetime as _dt  # BOILERPLATE — local import to avoid shadowing
        audit_writer.write_audit_record(
            db_conn=db_conn,
            filename=filename,
            desk_code=desk_code,
            trade_date=_dt.date.fromisoformat(trade_date_str),
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
            processing_ts_et=processing_ts_et,
        )
        db_conn.commit()
        logger.info("Audit record written with status=%s", status)

        # ── STEP 8: SNS success notification ────────────────────────────────
        summary = {
            "event_type": "TRADE_POSITIONS_LOADED",
            "filename": filename,
            "desk_code": desk_code,
            "trade_date": trade_date_str,
            "total_rows_received": total_rows,
            "rows_loaded": rows_inserted,
            "rows_rejected": rows_rejected,
            "rows_skipped_duplicate": len(valid_rows) - rows_inserted,
            "report_s3_key": report_key,
            "error_file_s3_key": error_file_key,
            "processing_timestamp_et": processing_ts_et.isoformat(),
        }
        sns_notifier.notify_success(sns_client, sns_success_arn, summary)
        logger.info("Success SNS notification sent to %s", sns_success_arn)

        return summary

    except Exception as exc:
        # LOGIC — on any pipeline stage failure: rollback, write audit FAILURE, re-raise
        logger.exception("Pipeline failure during processing of %s: %s", filename, exc)
        if db_conn is not None:
            try:
                db_conn.rollback()
            except Exception as rollback_exc:
                logger.warning("Rollback failed: %s", rollback_exc)

        # LOGIC — attempt audit write even on failure (best-effort; uses fresh transaction)
        if db_conn is not None:
            try:
                import datetime as _dt  # BOILERPLATE
                trade_date_obj = None
                if trade_date_str is not None:
                    try:
                        trade_date_obj = _dt.date.fromisoformat(trade_date_str)
                    except ValueError:
                        trade_date_obj = None

                audit_writer.write_audit_record(
                    db_conn=db_conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date=trade_date_obj,
                    status="FAILURE",
                    total_rows=total_rows,
                    rows_inserted=0,
                    rows_rejected=rows_rejected,
                    error_message=str(exc),
                    processing_ts_et=processing_ts_et,
                )
                db_conn.commit()
                logger.info("Failure audit record written")
            except Exception as audit_exc:
                logger.warning("Failed to write failure audit record: %s", audit_exc)

        raise  # LOGIC — re-raise so handler() can route to failure SNS

    finally:
        # BOILERPLATE — always close the DB connection
        if db_conn is not None:
            try:
                db_conn.close()
                logger.info("DB connection closed")
            except Exception as close_exc:
                logger.warning("Failed to close DB connection: %s", close_exc)