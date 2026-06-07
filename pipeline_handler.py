# BOILERPLATE
import logging
import re
from datetime import datetime

import pytz

import audit_writer
import error_writer
import file_reader
import notification_publisher
import pipeline_config
import position_loader
import report_builder
import row_validator
import secret_loader

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")

# LOGIC — pattern enforces incoming/ prefix and exact filename structure
_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>[^/]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — capture processing start time in ET immediately
    processed_at = datetime.now(_ET)

    # LOGIC — extract S3 event metadata
    try:
        s3_record = event["Records"][0]["s3"]
        bucket = s3_record["bucket"]["name"]
        # S3 keys may be URL-encoded; decode the + and %XX sequences
        import urllib.parse
        s3_key = urllib.parse.unquote_plus(s3_record["object"]["key"])
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event — cannot extract bucket/key. event=%s", event, exc_info=True)
        raise ValueError("Malformed S3 event structure") from exc

    # LOGIC — validate key pattern before any processing
    match = _KEY_PATTERN.match(s3_key)
    if not match:
        logger.error(
            "S3 key does not match expected pattern 'incoming/{desk_code}_{trade_date}_positions.csv'. "
            "key=%s",
            s3_key,
        )
        # Do not raise here; return gracefully so Lambda does not retry an unfixable bad key
        return {"statusCode": 400, "body": "S3 key pattern mismatch — skipped"}

    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    file_name = s3_key.split("/")[-1]

    logger.info(
        "Pipeline invoked. bucket=%s key=%s desk_code=%s trade_date=%s",
        bucket,
        s3_key,
        desk_code,
        trade_date,
    )

    # LOGIC — variables that may be populated before an exception is raised;
    # used in the failure audit payload when available
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    error_s3_key = None
    report_s3_key = None
    credentials = None

    try:
        # Step 1 — retrieve DB credentials from Secrets Manager
        credentials = secret_loader.get_db_credentials(pipeline_config.DB_SECRET_ID)

        # Step 2 — read raw CSV from S3
        raw_df = file_reader.read_position_file(bucket, s3_key)

        # Step 3 — validate rows; split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(raw_df)

        # Step 4 — write rejected rows to S3 error file (no-op if empty)
        error_s3_key = error_writer.write_error_file(
            rejected_df, bucket, desk_code, trade_date
        )

        # Step 5 — bulk-load valid rows into DB (idempotent upsert)
        rows_inserted = position_loader.load_positions(valid_df, credentials)

        # Step 6 — build summary report dict
        report = report_builder.build_report(
            raw_df, valid_df, rejected_df, rows_inserted, desk_code, trade_date
        )

        # LOGIC — attach fields needed by SNS payload and audit that report_builder
        # does not set (report_builder only knows about DataFrames and counts)
        report["file_name"] = file_name
        report["desk_code"] = desk_code
        report["trade_date"] = trade_date

        # Step 7 — serialise report to S3
        report_s3_key = report_builder.write_report(report, bucket, desk_code, trade_date)
        report["report_s3_key"] = report_s3_key

        # Step 8 — publish success SNS notification
        notification_publisher.publish_success(
            pipeline_config.SNS_TOPIC_ARN_SUCCESS, report
        )

        # Step 9 — write audit record (SUCCESS)
        audit_payload = {
            "file_name": file_name,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows": len(raw_df),
            "rows_loaded": rows_inserted,
            "rows_rejected": len(rejected_df),
            "processing_status": "SUCCESS",
            "error_file_s3_key": error_s3_key,
            "report_s3_key": report_s3_key,
            "processed_at": processed_at,
            "service_identity": _read_service_identity(),
        }
        audit_writer.write_audit_record(credentials, audit_payload)

        logger.info(
            "Pipeline completed successfully. desk_code=%s trade_date=%s "
            "rows_inserted=%d rows_rejected=%d",
            desk_code,
            trade_date,
            rows_inserted,
            len(rejected_df),
        )
        return {"statusCode": 200, "body": "OK"}

    except Exception as exc:
        # LOGIC — log the exception, notify failure topic, write failure audit,
        # then re-raise so Lambda marks the invocation as failed
        logger.exception(
            "Pipeline failed. bucket=%s key=%s desk_code=%s trade_date=%s error=%s",
            bucket,
            s3_key,
            desk_code,
            trade_date,
            str(exc),
        )

        # Publish failure notification (does not raise on SNS error)
        notification_publisher.publish_failure(
            pipeline_config.SNS_TOPIC_ARN_FAILURE,
            {
                "file_name": file_name,
                "desk_code": desk_code,
                "trade_date": trade_date,
                "error_message": f"{type(exc).__name__}: {exc}",
            },
        )

        # Write failure audit record — use sentinel zeroes for counts not yet computed
        if credentials is not None:
            failure_audit_payload = {
                "file_name": file_name,
                "desk_code": desk_code,
                "trade_date": trade_date,
                "total_rows": len(raw_df) if raw_df is not None else 0,
                "rows_loaded": rows_inserted,
                "rows_rejected": len(rejected_df) if rejected_df is not None else 0,
                "processing_status": "FAILURE",
                "error_file_s3_key": error_s3_key,
                "report_s3_key": report_s3_key,
                "processed_at": processed_at,
                "service_identity": _read_service_identity(),
            }
            try:
                audit_writer.write_audit_record(credentials, failure_audit_payload)
            except Exception:
                logger.error(
                    "Failed to write failure audit record. desk_code=%s trade_date=%s",
                    desk_code,
                    trade_date,
                    exc_info=True,
                )
        else:
            logger.error(
                "Credentials were never retrieved — cannot write failure audit record. "
                "desk_code=%s trade_date=%s",
                desk_code,
                trade_date,
            )

        raise


def _read_service_identity() -> str:
    # LOGIC — read SERVICE_IDENTITY from environment at call time; do not cache
    import os
    identity = os.environ.get("SERVICE_IDENTITY", "unknown")
    return identity