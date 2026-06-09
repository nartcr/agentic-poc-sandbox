# BOILERPLATE
import json
import logging
import os
import re
import urllib.parse

import pytz  # BOILERPLATE

# BOILERPLATE — sibling module imports (all modules assumed to exist)
import file_reader
import row_validator
import position_loader
import error_file_writer
import report_builder
import sns_notifier
import audit_logger

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — regex that matches EXACTLY the documented incoming key pattern:
# incoming/{desk_code}_{trade_date}_positions.csv
# desk_code may contain letters, digits, and hyphens; trade_date is YYYY-MM-DD.
_S3_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>[A-Za-z0-9\-]+)_(?P<trade_date>[0-9]{4}-[0-9]{2}-[0-9]{2})_positions\.csv$"
)


def _parse_s3_key(key: str) -> tuple:
    # LOGIC — validate key matches expected pattern and extract named groups
    match = _S3_KEY_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            "'incoming/{{desk_code}}_{{YYYY-MM-DD}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date_str = match.group("trade_date")
    return desk_code, trade_date_str


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — extract S3 event coordinates
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    # S3 keys can be URL-encoded in the event payload
    raw_key = record["s3"]["object"]["key"]
    key = urllib.parse.unquote_plus(raw_key)

    logger.info("Received S3 event: bucket=%s key=%s", bucket, key)

    desk_code = None
    trade_date_str = None

    try:
        # LOGIC — parse key to extract desk_code and trade_date
        desk_code, trade_date_str = _parse_s3_key(key)
        logger.info("Parsed key: desk_code=%s trade_date=%s", desk_code, trade_date_str)

        # LOGIC — stage 1: read raw CSV from S3
        raw_df, total_row_count = file_reader.read_position_file(bucket, key)
        logger.info("File read: %d rows", total_row_count)

        # LOGIC — stage 2: validate rows, split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: %d valid, %d rejected",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — stage 3: write rejected rows to S3 error file (only if any exist)
        error_s3_key = None
        if rows_rejected > 0:
            error_s3_key = error_file_writer.write_error_file(rejected_df, key)
            logger.info("Error file written: %s", error_s3_key)

        # LOGIC — stage 4: load valid rows into trade_positions (idempotent)
        rows_inserted = 0
        if not valid_df.empty:
            rows_inserted = position_loader.load_positions(valid_df)
        logger.info("Rows inserted into trade_positions: %d", rows_inserted)

        # LOGIC — stage 5: build summary report and manifest, write both to S3
        report = report_builder.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date_str,
            rows_inserted=rows_inserted,
            error_s3_key=error_s3_key,
        )
        report_s3_key = report.get("report_s3_key")
        manifest_s3_key = report.get("manifest_s3_key")
        logger.info("Report written: %s  Manifest: %s", report_s3_key, manifest_s3_key)

        # LOGIC — stage 6: publish success SNS notification
        sns_notifier.notify_success(report)
        logger.info("Success notification published")

        # LOGIC — determine audit status: PARTIAL if any rows rejected, else SUCCESS
        status = "PARTIAL" if rows_rejected > 0 else "SUCCESS"

        # LOGIC — stage 7: write audit record (always executed)
        audit_logger.write_audit_record(
            filename=key,
            desk_code=desk_code,
            trade_date=trade_date_str,
            status=status,
            total_rows=total_row_count,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
        )
        logger.info("Audit record written: status=%s", status)

        # LOGIC — return structured JSON body as required by QA tests
        response_body = {
            "rows_inserted": rows_inserted,
            "rows_rejected": rows_rejected,
            "error_file": error_s3_key,
            "report_file": report_s3_key,
        }
        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as exc:  # LOGIC — catch-all for unhandled pipeline failures
        error_detail = str(exc)
        logger.exception("Unhandled pipeline failure for key=%s: %s", key, error_detail)

        # LOGIC — publish failure notification with whatever context we have
        try:
            sns_notifier.notify_failure(
                filename=key,
                error_detail=error_detail,
                desk_code=desk_code,
                trade_date=trade_date_str,
            )
        except Exception as sns_exc:  # BOILERPLATE — swallow SNS errors so audit still runs
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        # LOGIC — always attempt to write FAILURE audit record
        try:
            audit_logger.write_audit_record(
                filename=key,
                desk_code=desk_code,
                trade_date=trade_date_str,
                status="FAILURE",
                total_rows=0,
                rows_inserted=0,
                rows_rejected=0,
                error_message=error_detail,
            )
        except Exception as audit_exc:  # BOILERPLATE — swallow audit errors; do not mask original
            logger.error("Failed to write FAILURE audit record: %s", audit_exc)

        return {"statusCode": 500, "body": json.dumps({"error": error_detail})}