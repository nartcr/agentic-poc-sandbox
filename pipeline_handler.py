# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

import db_loader
import file_validator
import report_builder
import s3_client
import sns_notifier
import timestamp_helper

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# desk_code may itself contain underscores, so anchor on the date portion (YYYY-MM-DD) and suffix
_FILENAME_RE = re.compile(
    r"^(?:.*/)?"                        # optional path prefix (e.g. "incoming/")
    r"(?P<desk_code>.+?)"               # desk_code: non-greedy, everything before the date
    r"_(?P<trade_date>\d{4}-\d{2}-\d{2})"  # literal underscore + YYYY-MM-DD
    r"_positions\.csv$"                 # literal suffix
)


def _extract_s3_event(event: dict) -> tuple:
    # LOGIC — extract bucket and key from S3 event record
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    logger.info("Extracted S3 event: bucket=%s key=%s", bucket, key)
    return bucket, key


def _parse_filename(key: str) -> tuple:
    # LOGIC — parse desk_code and trade_date from S3 key using regex; never str.split
    match = _FILENAME_RE.match(key)
    if not match:
        raise ValueError(
            f"Filename key '{key}' does not match expected pattern "
            f"'{{desk_code}}_{{YYYY-MM-DD}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date_str = match.group("trade_date")
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date_str)
    return desk_code, trade_date_str


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — capture pipeline start time in ET immediately
    processing_timestamp_et: datetime = timestamp_helper.now_et()

    # LOGIC — state variables used in finally block for audit
    filename: str = ""
    desk_code: str | None = None
    trade_date_str: str | None = None
    status: str = "FAILURE"
    total_rows: int = 0
    rows_inserted: int = 0
    rows_rejected: int = 0
    error_message: str | None = None
    error_file_key: str | None = None
    report_file_key: str | None = None

    try:
        # LOGIC — step 1: extract S3 event
        bucket, key = _extract_s3_event(event)
        filename = key

        # LOGIC — step 2: parse filename to get desk_code and trade_date
        desk_code, trade_date_str = _parse_filename(key)

        # LOGIC — step 3: download file content from S3
        logger.info("Downloading file from S3: bucket=%s key=%s", bucket, key)
        csv_content: str = s3_client.download_file(bucket, key)

        # LOGIC — step 4: validate rows; split into valid and rejected sets
        logger.info("Validating rows for desk_code=%s trade_date=%s", desk_code, trade_date_str)
        valid_df, rejected_df = file_validator.validate_rows(
            csv_content, desk_code, trade_date_str
        )

        total_rows = len(valid_df) + len(rejected_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: total=%d valid=%d rejected=%d",
            total_rows, len(valid_df), rows_rejected,
        )

        # LOGIC — step 5: upload rejected rows to S3 error prefix (always, even if empty)
        error_file_key = f"errors/{desk_code}_{trade_date_str}_rejected.csv"
        if rows_rejected > 0:
            logger.info("Writing %d rejected rows to %s", rows_rejected, error_file_key)
            s3_client.upload_file(
                bucket,
                error_file_key,
                rejected_df.to_csv(index=False),
                content_type="text/csv",
            )
        else:
            logger.info("No rejected rows; skipping error file upload")
            error_file_key = None  # LOGIC — null in response when no errors

        # LOGIC — step 6: load valid rows into Aurora; idempotent via ON CONFLICT DO NOTHING
        if len(valid_df) > 0:
            logger.info("Loading %d valid rows into database", len(valid_df))
            rows_inserted = db_loader.load_positions(valid_df)
            logger.info("Rows inserted (dedup-aware): %d", rows_inserted)
        else:
            rows_inserted = 0
            logger.warning("No valid rows to insert for desk_code=%s trade_date=%s", desk_code, trade_date_str)

        # LOGIC — step 7: build summary report
        logger.info("Building summary report")
        report: dict = report_builder.build_report(
            filename=key,
            desk_code=desk_code,
            trade_date=trade_date_str,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            processing_timestamp_et=processing_timestamp_et,
        )

        # LOGIC — step 8: upload summary report JSON to S3 reports prefix
        ts_suffix = processing_timestamp_et.strftime("%Y%m%dT%H%M%S")
        report_file_key = f"reports/{desk_code}_{trade_date_str}_summary_{ts_suffix}.json"
        logger.info("Uploading report to %s", report_file_key)
        s3_client.upload_file(
            bucket,
            report_file_key,
            json.dumps(report, indent=2, default=str),
            content_type="application/json",
        )

        # LOGIC — step 9: write manifest so downstream consumers can find files at a known key
        manifest_key = f"manifests/{desk_code}_{trade_date_str}_manifest.json"
        manifest: dict = {
            "desk_code": desk_code,
            "trade_date": trade_date_str,
            "input_key": key,
            "error_key": error_file_key if error_file_key else "",
            "report_key": report_file_key,
            "generated_at_et": timestamp_helper.to_et_string(processing_timestamp_et),
        }
        logger.info("Writing manifest to %s", manifest_key)
        s3_client.write_manifest(bucket, manifest_key, manifest)

        # LOGIC — step 10: determine final status
        if rows_rejected > 0 and rows_inserted == 0:
            status = "PARTIAL"
        elif rows_rejected > 0:
            status = "PARTIAL"
        else:
            status = "SUCCESS"

        # LOGIC — step 11: publish success SNS notification
        # Augment report with S3 key pointers required by SNS success payload contract
        sns_payload: dict = dict(report)
        sns_payload["event_type"] = "TRADE_POSITIONS_LOADED"
        sns_payload["report_s3_key"] = report_file_key
        sns_payload["manifest_s3_key"] = manifest_key
        logger.info("Publishing success SNS notification")
        sns_notifier.notify_success(sns_payload)

        # LOGIC — return structured JSON body as required by Lambda response contract
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_file_key,
                    "report_file": report_file_key,
                }
            ),
        }

    except Exception as exc:
        # LOGIC — catch-all: record failure details, publish failure SNS
        error_message = str(exc)
        status = "FAILURE"
        logger.exception("Pipeline failed for file '%s': %s", filename, error_message)

        try:
            sns_notifier.notify_failure(
                filename=filename,
                error_message=error_message,
                processing_timestamp_et=processing_timestamp_et,
            )
        except Exception as sns_exc:
            # LOGIC — SNS failure must not suppress the original error path
            logger.exception("Failed to publish failure SNS notification: %s", sns_exc)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_file_key,
                    "report_file": report_file_key,
                    "error_message": error_message,
                }
            ),
        }

    finally:
        # LOGIC — unconditionally write audit record regardless of outcome (BAC-7, TAC-7)
        try:
            db_loader.write_audit_record(
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date_str,
                status=status,
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=error_message,
                processing_timestamp_et=processing_timestamp_et,
            )
            logger.info(
                "Audit record written: status=%s total=%d inserted=%d rejected=%d",
                status, total_rows, rows_inserted, rows_rejected,
            )
        except Exception as audit_exc:
            # LOGIC — audit failure must not alter the response already determined above
            logger.exception("Failed to write audit record: %s", audit_exc)