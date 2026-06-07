# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

# BOILERPLATE
import file_reader
import row_validator
import position_loader
import report_builder
import report_writer
import error_writer
import audit_writer
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex anchored to the exact S3 key pattern: incoming/{desk_code}_{YYYY-MM-DD}_positions.csv
# desk_code may itself contain underscores, so we anchor on the date segment (YYYY-MM-DD) and the literal suffix.
_FILENAME_RE = re.compile(
    r"^incoming/(.+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _now_et() -> datetime:
    # LOGIC — all timestamps in America/Toronto per global rules
    tz_et = pytz.timezone("America/Toronto")
    return datetime.now(tz_et)


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — extract S3 event fields
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info("Pipeline triggered for bucket=%s key=%s", bucket, key)

    # LOGIC — capture processing timestamp once; thread through all calls
    processing_timestamp_et = _now_et()
    processing_timestamp_et_iso = processing_timestamp_et.isoformat()

    desk_code: str | None = None
    trade_date: str | None = None

    # LOGIC — parse desk_code and trade_date from filename using regex (never split on underscore)
    match = _FILENAME_RE.match(key)
    if match:
        desk_code = match.group(1)
        trade_date = match.group(2)
        logger.info("Parsed desk_code=%s trade_date=%s from key=%s", desk_code, trade_date, key)
    else:
        logger.warning("Key %s does not match expected filename pattern; desk_code and trade_date will be null", key)

    total_rows = 0
    rows_inserted = 0
    rows_rejected = 0
    error_file_key = None
    report_s3_key = None

    try:
        # LOGIC — Step 1: read CSV from S3
        logger.info("Reading CSV from s3://%s/%s", bucket, key)
        raw_df, total_rows = file_reader.read_csv_from_s3(bucket, key)
        logger.info("Read %d rows from file", total_rows)

        # LOGIC — Step 2: validate rows
        logger.info("Validating rows")
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        rows_rejected = len(rejected_df)
        logger.info("Validation complete: valid=%d rejected=%d", len(valid_df), rows_rejected)

        # LOGIC — Step 3: load valid rows into database
        logger.info("Loading %d valid rows into database", len(valid_df))
        rows_inserted = position_loader.load_positions(valid_df)
        logger.info("Inserted %d rows into trade_positions", rows_inserted)

        # LOGIC — Step 4: build summary report
        logger.info("Building summary report")
        report = report_builder.build_report(
            total_rows=total_rows,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code or "",
            trade_date=trade_date or "",
        )

        # LOGIC — Step 5: write JSON report and manifest to S3
        logger.info("Writing report to S3")
        report_s3_key = report_writer.write_report(
            report=report,
            desk_code=desk_code or "",
            trade_date=trade_date or "",
            processing_timestamp_et=processing_timestamp_et_iso,
        )
        logger.info("Report written to key=%s", report_s3_key)

        # LOGIC — Step 6: write error CSV to S3 (always written, may be header-only)
        logger.info("Writing error file to S3")
        error_file_key = error_writer.write_error_file(
            rejected_df=rejected_df,
            desk_code=desk_code or "",
            trade_date=trade_date or "",
            processing_timestamp_et=processing_timestamp_et_iso,
        )
        logger.info("Error file written to key=%s", error_file_key)

        # LOGIC — determine pipeline status
        if rows_rejected == 0:
            status = "SUCCESS"
        else:
            status = "PARTIAL"
        logger.info("Pipeline status=%s", status)

        # LOGIC — Step 7: write audit record
        audit_writer.write_audit_record(
            filename=key,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
            processing_timestamp_et=processing_timestamp_et,
        )
        logger.info("Audit record written with status=%s", status)

        # LOGIC — Step 8: publish SNS success notification
        # Derive manifest key for notification (predictable, no timestamp)
        manifest_key = "manifests/{desk_code}_{trade_date}_manifest.json".format(
            desk_code=desk_code or "",
            trade_date=trade_date or "",
        )
        report["report_key"] = report_s3_key
        report["manifest_key"] = manifest_key
        report["filename"] = key
        sns_notifier.notify_success(report)
        logger.info("SNS success notification published")

        # BOILERPLATE — structured Lambda response
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file_key": error_file_key,
            "report_s3_key": report_s3_key,
            "status": status,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows": total_rows,
        }
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:  # LOGIC — catch-all: write failed audit and SNS failure
        logger.exception("Unhandled exception processing key=%s: %s", key, exc)

        # LOGIC — write FAILED audit record
        try:
            audit_writer.write_audit_record(
                filename=key,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=str(exc),
                processing_timestamp_et=processing_timestamp_et,
            )
        except Exception as audit_exc:
            logger.error("Failed to write audit record after pipeline failure: %s", audit_exc)

        # LOGIC — publish SNS failure notification
        try:
            sns_notifier.notify_failure(
                filename=key,
                error_message=str(exc),
                desk_code=desk_code,
                trade_date=trade_date,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish SNS failure notification: %s", sns_exc)

        # BOILERPLATE — structured Lambda response for failure path (statusCode 200, status=FAILED)
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file_key": error_file_key,
            "report_s3_key": report_s3_key,
            "status": "FAILED",
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows": total_rows,
            "error_message": str(exc),
        }
        return {"statusCode": 200, "body": json.dumps(response_body)}