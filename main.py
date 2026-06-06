# BOILERPLATE
import logging
import os
import re
import urllib.parse
from datetime import datetime

import pytz

from audit import record as audit_record
from exceptions import NotificationError
from file_reader import read_csv_from_s3
from loader import load_positions
from notifier import notify_failure, notify_success
from reporter import build_report, write_error_file, write_report
from validator import validate

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
ET = pytz.timezone("America/Toronto")

# LOGIC
_KEY_PATTERN = re.compile(r"^(?:.+/)?(?P<desk_code>[^/]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$")


def _extract_key_parts(key: str) -> tuple:
    """Parse desk_code and trade_date out of the S3 object key."""  # LOGIC
    match = _KEY_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 object key '{key}' does not match expected pattern "
            "'{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    return match.group("desk_code"), match.group("trade_date")


def _determine_outcome(rows_loaded: int, rows_rejected: int) -> str:
    """Map load/rejection counts to an audit outcome string."""  # LOGIC
    if rows_rejected == 0:
        return "SUCCESS"
    if rows_loaded > 0:
        return "PARTIAL"
    return "FAILURE"


def handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point.  Coordinates the full position ingestion pipeline
    for a single S3 file event.
    """  # LOGIC
    # LOGIC — extract S3 coordinates from the Lambda event
    record_s3 = event["Records"][0]["s3"]
    bucket = record_s3["bucket"]["name"]
    key = urllib.parse.unquote_plus(record_s3["object"]["key"])

    logger.info("Starting position ingestion for s3://%s/%s", bucket, key)

    desk_code, trade_date = _extract_key_parts(key)
    logger.info("Parsed desk_code=%s trade_date=%s from key", desk_code, trade_date)

    processing_timestamp = datetime.now(tz=ET)

    try:
        # LOGIC — step 1: read raw file
        raw_df = read_csv_from_s3(bucket, key)
        logger.info("Read %d raw rows from s3://%s/%s", len(raw_df), bucket, key)

        # LOGIC — step 2: validate
        valid_df, rejected_df = validate(raw_df, desk_code, trade_date)
        logger.info(
            "Validation complete: %d valid rows, %d rejected rows",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 3: load valid rows
        inserted_count = load_positions(valid_df, source_file=key)
        logger.info("Inserted %d rows into rfdh.trade_positions", inserted_count)

        # LOGIC — step 4: build and write report
        report_dict = build_report(
            raw_df,
            valid_df,
            rejected_df,
            inserted_count,
            desk_code,
            trade_date,
            source_file=key,
        )
        write_report(report_dict, desk_code, trade_date)
        logger.info("Summary report written for desk=%s trade_date=%s", desk_code, trade_date)

        # LOGIC — step 5: write rejection error file if any rejections exist
        if len(rejected_df) > 0:
            write_error_file(rejected_df, desk_code, trade_date)
            logger.info(
                "Error file written: %d rejected rows for desk=%s trade_date=%s",
                len(rejected_df),
                desk_code,
                trade_date,
            )

        # LOGIC — step 6: SNS success notification
        notify_success(report_dict)
        logger.info("Success notification published for desk=%s trade_date=%s", desk_code, trade_date)

        # LOGIC — step 7: audit record
        outcome = _determine_outcome(inserted_count, len(rejected_df))
        audit_record(
            source_file=key,
            outcome=outcome,
            total_rows=len(raw_df),
            rows_loaded=inserted_count,
            rows_rejected=len(rejected_df),
            processing_timestamp=processing_timestamp,
        )
        logger.info("Audit record written: outcome=%s", outcome)

        return {
            "statusCode": 200,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows": len(raw_df),
            "rows_loaded": inserted_count,
            "rows_rejected": len(rejected_df),
            "outcome": outcome,
        }

    except Exception as exc:  # LOGIC — unhandled exception path
        logger.exception(
            "Unhandled exception during ingestion of s3://%s/%s: %s",
            bucket,
            key,
            str(exc),
        )

        # LOGIC — attempt failure notification; swallow NotificationError per design
        try:
            notify_failure(source_file=key, error=str(exc))
        except NotificationError as notify_exc:
            logger.error(
                "Failed to publish failure notification for key=%s: %s",
                key,
                str(notify_exc),
            )

        # LOGIC — write audit record for failure path
        try:
            audit_record(
                source_file=key,
                outcome="FAILURE",
                total_rows=0,
                rows_loaded=0,
                rows_rejected=0,
                processing_timestamp=processing_timestamp,
            )
        except Exception as audit_exc:
            logger.error(
                "Failed to write failure audit record for key=%s: %s",
                key,
                str(audit_exc),
            )

        raise