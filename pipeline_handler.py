# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

import file_parser
import row_validator
import db_loader
import error_writer
import report_builder
import audit_writer
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex matches incoming/{desk_code}_{trade_date}_positions.csv
# desk_code: alphanumeric + hyphens (no underscores — underscore is the delimiter)
# trade_date: YYYY-MM-DD
_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>[A-Za-z0-9\-]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


def _now_et() -> datetime:
    # LOGIC — single authoritative ET timestamp source
    return datetime.now(_ET)


def handler(event: dict, context) -> dict:  # noqa: ANN001
    """Lambda entry point. Receives S3 ObjectCreated event."""
    # BOILERPLATE
    processing_timestamp_et: datetime = _now_et()

    # LOGIC — extract S3 coordinates from event
    try:
        s3_record = event["Records"][0]["s3"]
        bucket_name: str = s3_record["bucket"]["name"]
        object_key: str = s3_record["object"]["key"]
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event payload: %s", exc)
        _try_notify_failure(
            filename="<unknown>",
            error=f"Malformed S3 event payload: {exc}",
            processing_timestamp_et=processing_timestamp_et,
        )
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "rows_rejected": 0,
                    "error_file": None,
                    "report_file": None,
                    "error": f"Malformed S3 event payload: {exc}",
                }
            ),
        }

    logger.info("Pipeline triggered for s3://%s/%s", bucket_name, object_key)

    # LOGIC — validate filename convention using regex (never str.split)
    match = _KEY_PATTERN.match(object_key)
    if not match:
        error_msg = (
            f"Object key '{object_key}' does not match expected pattern "
            r"incoming/<desk_code>_<YYYY-MM-DD>_positions.csv"
        )
        logger.error(error_msg)
        _try_audit_failed(
            filename=object_key,
            desk_code=None,
            trade_date=None,
            error_message=error_msg,
            processing_timestamp_et=processing_timestamp_et,
        )
        _try_notify_failure(
            filename=object_key,
            error=error_msg,
            processing_timestamp_et=processing_timestamp_et,
        )
        return {
            "statusCode": 400,
            "body": json.dumps(
                {
                    "status": "FAILED",
                    "rows_inserted": 0,
                    "rows_rejected": 0,
                    "error_file": None,
                    "report_file": None,
                    "error": error_msg,
                }
            ),
        }

    desk_code: str = match.group("desk_code")
    trade_date: str = match.group("trade_date")
    filename: str = object_key

    logger.info("Parsed desk_code=%s trade_date=%s", desk_code, trade_date)

    # LOGIC — initialise result sentinels so the except block can always reference them
    rows_inserted: int = 0
    rows_rejected: int = 0
    error_s3_key: str | None = None
    report_s3_key: str | None = None

    try:
        # LOGIC — Stage 1: parse CSV from S3
        logger.info("Stage 1: parsing CSV")
        raw_df = file_parser.parse_s3_csv(bucket_name, object_key)
        logger.info("Parsed %d raw rows", len(raw_df))

        # LOGIC — Stage 2: validate rows
        logger.info("Stage 2: validating rows")
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — Stage 3: load valid rows to DB
        logger.info("Stage 3: loading positions to DB")
        rows_inserted = db_loader.load_positions(valid_df)
        logger.info("DB load complete: rows_inserted=%d", rows_inserted)

        # LOGIC — Stage 4: write error file (always written, even if empty)
        logger.info("Stage 4: writing error file")
        error_s3_key = error_writer.write_error_file(rejected_df, desk_code, trade_date)
        logger.info("Error file written: %s", error_s3_key)

        # LOGIC — Stage 5: build and write report + manifest
        logger.info("Stage 5: building report")
        report_s3_key = report_builder.build_and_write_report(
            valid_df=valid_df,
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            rows_inserted=rows_inserted,
            processing_timestamp_et=processing_timestamp_et,
        )
        logger.info("Report written: %s", report_s3_key)

        # LOGIC — Stage 6: write audit record (SUCCESS)
        total_rows: int = len(valid_df) + rows_rejected
        logger.info("Stage 6: writing audit record (SUCCESS)")
        audit_writer.write_audit_record(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status="SUCCESS",
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
            processing_timestamp_et=processing_timestamp_et,
        )

        # LOGIC — Stage 7: publish SNS success notification
        rows_skipped_duplicate: int = len(valid_df) - rows_inserted
        logger.info("Stage 7: publishing SNS success notification")
        sns_notifier.notify_success(
            {
                "event": "TRADE_POSITIONS_LOADED",
                "desk_code": desk_code,
                "trade_date": trade_date,
                "rows_loaded": rows_inserted,
                "rows_rejected": rows_rejected,
                "rows_skipped_duplicate": rows_skipped_duplicate,
                "report_s3_key": report_s3_key,
                "processing_timestamp_et": processing_timestamp_et.isoformat(),
            }
        )

        logger.info(
            "Pipeline SUCCESS: rows_inserted=%d rows_rejected=%d",
            rows_inserted,
            rows_rejected,
        )
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "status": "SUCCESS",
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_s3_key,
                    "report_file": report_s3_key,
                }
            ),
        }

    except Exception as exc:  # noqa: BLE001
        error_msg = str(exc)
        logger.exception(
            "Pipeline FAILED for %s: %s", object_key, error_msg
        )

        # LOGIC — audit + notify on unhandled exception; swallow secondary errors
        _try_audit_failed(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            error_message=error_msg,
            processing_timestamp_et=processing_timestamp_et,
        )
        _try_notify_failure(
            filename=filename,
            error=error_msg,
            processing_timestamp_et=processing_timestamp_et,
        )

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "status": "FAILED",
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_s3_key,
                    "report_file": report_s3_key,
                    "error": error_msg,
                }
            ),
        }


# LOGIC — helpers to swallow secondary failures so the primary error is not masked

def _try_audit_failed(
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    error_message: str,
    processing_timestamp_et: datetime,
) -> None:
    """Attempt to write a FAILED audit record; log and continue on error."""
    try:
        audit_writer.write_audit_record(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status="FAILED",
            total_rows=0,
            rows_inserted=0,
            rows_rejected=0,
            error_message=error_message,
            processing_timestamp_et=processing_timestamp_et,
        )
    except Exception as secondary:  # noqa: BLE001
        logger.error("Failed to write audit record: %s", secondary)


def _try_notify_failure(
    filename: str,
    error: str,
    processing_timestamp_et: datetime,
) -> None:
    """Attempt to publish SNS failure notification; log and continue on error."""
    try:
        sns_notifier.notify_failure(
            {
                "event": "TRADE_POSITIONS_FAILED",
                "filename": filename,
                "error": error,
                "processing_timestamp_et": processing_timestamp_et.isoformat(),
            }
        )
    except Exception as secondary:  # noqa: BLE001
        logger.error("Failed to publish SNS failure notification: %s", secondary)