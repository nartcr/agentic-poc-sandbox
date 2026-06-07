# BOILERPLATE
import json
import logging
import os
import re
from urllib.parse import unquote_plus

import file_reader
import row_validator
import position_loader
import report_builder
import audit_writer
import sns_notifier
from ingestion_exceptions import FilenameParseError

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# trade_date is YYYY-MM-DD; desk_code may contain uppercase letters/digits but NOT underscores
# Anchoring trade_date to \d{4}-\d{2}-\d{2} makes the split unambiguous without relying on str.split('_')
_FILENAME_RE = re.compile(
    r"^(?:incoming/)?(?P<desk_code>[A-Za-z0-9]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_filename(key: str) -> tuple:
    """
    # LOGIC
    Parse desk_code and trade_date from an S3 object key of the form
    incoming/{desk_code}_{trade_date}_positions.csv.

    Raises FilenameParseError if the key does not match the expected pattern.
    Returns (desk_code, trade_date) as strings.
    """
    match = _FILENAME_RE.match(key)
    if not match:
        raise FilenameParseError(
            f"S3 key '{key}' does not match expected pattern "
            "'incoming/{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date)
    return desk_code, trade_date


def handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    Lambda entry point. Orchestrates the full ingestion pipeline:
      1. Extract S3 bucket/key from event
      2. Parse desk_code and trade_date from filename
      3. Read CSV from S3
      4. Validate rows → (valid_df, rejected_df)
      5. Load valid rows into DB
      6. Build and store summary report + manifest
      7. Write audit record
      8. Publish SNS success notification
    On any exception, write FAILED audit record and publish SNS failure notification.
    """
    # BOILERPLATE — extract S3 coordinates from event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    raw_key = record["s3"]["object"]["key"]
    # AWS URL-encodes keys with spaces/special chars; decode before use
    key = unquote_plus(raw_key)

    logger.info("Lambda invoked: bucket=%s key=%s", bucket, key)

    # LOGIC — initialise tracking variables used in both happy-path and error handler
    filename = key
    desk_code = None
    trade_date = None
    total_rows = 0
    rows_inserted = 0
    rows_rejected = 0
    error_file_key = None
    report_file_key = None

    try:
        # LOGIC — Step 1: parse structured filename
        desk_code, trade_date = _parse_filename(key)

        # LOGIC — Step 2: read raw CSV from S3 into DataFrame (all str dtypes)
        raw_df = file_reader.read_s3_csv(bucket, key)
        total_rows = len(raw_df)
        logger.info("File read: total_rows=%d", total_rows)

        # LOGIC — Step 3: validate rows; rejected rows written to S3 by row_validator
        valid_df, rejected_df = row_validator.validate_rows(
            raw_df, desk_code, trade_date, bucket
        )
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — Step 4: load valid rows into demo_schema.trade_positions (idempotent)
        rows_inserted = position_loader.load_positions(valid_df)
        logger.info("DB load complete: rows_inserted=%d", rows_inserted)

        # LOGIC — Step 5: build summary report + manifest and write to S3
        report, report_file_key = report_builder.build_and_store_report(
            bucket=bucket,
            filename=key,
            desk_code=desk_code,
            trade_date=trade_date,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            valid_df=valid_df,
            rejected_df=rejected_df,
        )
        logger.info("Report written: report_key=%s", report_file_key)

        # LOGIC — derive error file key from the known S3 path convention
        error_file_key = f"errors/{desk_code}_{trade_date}_rejected.csv"

        # LOGIC — Step 6: determine overall status
        if rows_rejected == 0:
            status = "SUCCESS"
        elif rows_inserted > 0:
            status = "PARTIAL"
        else:
            # All rows rejected — still SUCCESS at pipeline level (no system error)
            status = "PARTIAL"

        # LOGIC — Step 7: write audit record
        audit_writer.write_audit_record(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
        )
        logger.info("Audit record written: status=%s", status)

        # LOGIC — Step 8: notify downstream pipeline via SNS success topic
        summary_dict = {
            "event": "TRADE_POSITIONS_LOADED",
            "desk_code": desk_code,
            "trade_date": trade_date,
            "total_rows_received": total_rows,
            "rows_successfully_loaded": rows_inserted,
            "rows_rejected": rows_rejected,
            "processing_timestamp_et": report.get("processing_timestamp_et"),
            "report_s3_key": report_file_key,
            "manifest_s3_key": f"manifests/{desk_code}_{trade_date}_manifest.json",
        }
        sns_notifier.notify_success(desk_code, trade_date, summary_dict)
        logger.info("SNS success notification published")

        # BOILERPLATE — return standardised Lambda response
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

    except Exception as exc:  # LOGIC — catch-all: write FAILED audit, notify failure SNS
        logger.exception(
            "Unhandled exception processing file '%s': %s", filename, exc
        )

        error_detail = str(exc)

        # LOGIC — attempt audit write; if it also fails, log and continue to SNS
        try:
            audit_writer.write_audit_record(
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=error_detail,
            )
        except Exception as audit_exc:
            logger.error("Failed to write audit record: %s", audit_exc)

        # LOGIC — publish failure notification so ops team is alerted
        try:
            sns_notifier.notify_failure(filename, error_detail)
        except Exception as sns_exc:
            logger.error("Failed to publish SNS failure notification: %s", sns_exc)

        # LOGIC — re-raise so Lambda marks invocation as failed and CloudWatch alarm fires
        raise