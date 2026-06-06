# BOILERPLATE
import logging
import urllib.parse
import pytz
from datetime import datetime

# BOILERPLATE
import s3_reader
import validator
import loader
import reporter
import s3_writer
import notifier
import audit

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def _parse_key(key: str) -> tuple:
    # LOGIC
    # Expected pattern: incoming/{desk_code}_{trade_date}_positions.csv
    # trade_date is always YYYY-MM-DD (10 chars), positions is a fixed suffix.
    # Strategy: strip prefix dir, strip "_positions.csv" suffix, then split
    # on the last occurrence of "_" before the date portion.
    filename = key.split("/")[-1]  # strip any directory prefix
    suffix = "_positions.csv"
    if not filename.endswith(suffix):
        raise ValueError(
            f"S3 key filename '{filename}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    stem = filename[: -len(suffix)]  # e.g. "DESKCODE_2024-01-15"
    # trade_date is always YYYY-MM-DD → last 10 chars after final "_"
    last_underscore = stem.rfind("_")
    if last_underscore == -1:
        raise ValueError(
            f"Cannot parse desk_code and trade_date from filename '{filename}'"
        )
    desk_code = stem[:last_underscore]
    trade_date = stem[last_underscore + 1:]
    if not desk_code:
        raise ValueError(f"Parsed desk_code is empty from filename '{filename}'")
    if not trade_date:
        raise ValueError(f"Parsed trade_date is empty from filename '{filename}'")
    logger.info("Parsed desk_code='%s', trade_date='%s' from key '%s'",
                desk_code, trade_date, key)
    return desk_code, trade_date


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — extract S3 event fields
    s3_record = event["Records"][0]["s3"]
    bucket = s3_record["bucket"]["name"]
    raw_key = s3_record["object"]["key"]
    key = urllib.parse.unquote_plus(raw_key)

    logger.info("Lambda triggered for s3://%s/%s", bucket, key)

    desk_code = None
    trade_date = None

    try:
        # LOGIC — Step 1: parse desk_code and trade_date from filename
        desk_code, trade_date = _parse_key(key)

        # LOGIC — Step 2: read raw CSV from S3
        raw_df = s3_reader.read_csv(bucket, key)
        logger.info("Read %d raw rows from s3://%s/%s", len(raw_df), bucket, key)

        # LOGIC — Step 3: validate rows
        valid_df, rejected_df = validator.validate(raw_df, desk_code, trade_date)
        logger.info(
            "Validation complete: %d valid, %d rejected",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — Step 4: write error file if any rows were rejected
        error_file_s3_key = None
        if not rejected_df.empty:
            error_file_s3_key = s3_writer.write_error_file(
                rejected_df, desk_code, trade_date
            )
            logger.info("Error file written to s3 key: %s", error_file_s3_key)

        # LOGIC — Step 5: load valid rows into Aurora
        rows_inserted = loader.load_positions(valid_df)
        logger.info("Loaded %d rows into trade_positions", rows_inserted)

        # LOGIC — Step 6: build summary report
        summary = reporter.build_summary(
            raw_df, valid_df, rejected_df, rows_inserted, desk_code, trade_date
        )
        # Attach the S3 keys so the notifier and callers have them
        summary["report_s3_key"] = (
            f"reports/{desk_code}_{trade_date}_summary.json"
        )
        summary["error_file_s3_key"] = error_file_s3_key
        summary["s3_input_key"] = key
        logger.info("Summary built: %s", summary)

        # LOGIC — Step 7: write summary report to S3
        report_key = s3_writer.write_report(summary, desk_code, trade_date)
        summary["report_s3_key"] = report_key  # use the key actually written
        logger.info("Summary report written to s3 key: %s", report_key)

        # LOGIC — Step 8: record audit entry
        audit.record(desk_code, trade_date, summary, status="SUCCESS")
        logger.info("Audit record written for desk_code='%s', trade_date='%s'",
                    desk_code, trade_date)

        # LOGIC — Step 9: send success notification
        notifier.notify_success(summary)
        logger.info("Success notification sent")

        return {"statusCode": 200, "body": summary}

    except Exception as exc:  # LOGIC — Step 10: failure path
        et_now = datetime.now(_ET).isoformat()
        logger.exception(
            "Pipeline failed for key '%s': %s", key, str(exc)
        )
        error_details = {
            "event_type": "TRADE_POSITION_INGESTION_FAILURE",
            "desk_code": desk_code,
            "trade_date": trade_date,
            "s3_input_key": key,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "processing_timestamp_et": et_now,
        }
        try:
            audit.record(
                desk_code,
                trade_date,
                summary={},
                status="FAILURE",
                error_message=str(exc),
            )
        except Exception as audit_exc:
            logger.error("Failed to write audit record on failure path: %s", audit_exc)
        try:
            notifier.notify_failure(error_details)
        except Exception as notify_exc:
            logger.error(
                "Failed to send failure notification: %s", notify_exc
            )
        raise