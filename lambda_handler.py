# BOILERPLATE
import json
import logging
import os
import re
from urllib.parse import unquote_plus

import pytz
from datetime import datetime

# BOILERPLATE — sub-module imports (all exist in same package)
import file_reader
import row_validator
import position_loader
import report_builder
import error_writer
import audit_logger
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern: {desk_code}_{trade_date}_positions.csv
# desk_code may contain alphanumerics; trade_date is YYYY-MM-DD
_FILENAME_RE = re.compile(
    r"^(?P<desk_code>[A-Za-z0-9]+)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _et_now_iso() -> str:
    # LOGIC — all timestamps must be Eastern Time (America/Toronto)
    tz_et = pytz.timezone("America/Toronto")
    return datetime.now(tz_et).isoformat()


def _parse_filename(key: str) -> tuple:
    # LOGIC — extract the bare filename from a potentially prefixed S3 key
    # e.g. "incoming/EQTY_2026-06-01_positions.csv" → "EQTY_2026-06-01_positions.csv"
    bare_filename = key.split("/")[-1]
    match = _FILENAME_RE.match(bare_filename)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            f"'{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    desk_code = match.group("desk_code")
    trade_date = match.group("trade_date")
    return bare_filename, desk_code, trade_date


def lambda_handler(event: dict, context) -> dict:  # LOGIC — main Lambda entry point
    processing_timestamp_et = _et_now_iso()

    # BOILERPLATE — state variables initialised before try so except block can reference them
    filename = None
    desk_code = None
    trade_date = None
    rows_inserted = 0
    rows_rejected = 0
    total_rows = 0
    report_s3_key = None
    error_s3_key = None

    try:
        # LOGIC — extract bucket and key from S3 event record
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        raw_key = record["s3"]["object"]["key"]
        # LOGIC — S3 event keys are URL-encoded; decode before use
        key = unquote_plus(raw_key)

        logger.info("Received S3 event: bucket=%s key=%s", bucket, key)

        # LOGIC — parse filename components via regex (never str.split)
        filename, desk_code, trade_date = _parse_filename(key)
        logger.info("Parsed filename=%s desk_code=%s trade_date=%s", filename, desk_code, trade_date)

        # LOGIC — step 1: read raw CSV from S3
        raw_df = file_reader.read_csv_from_s3(bucket, key)
        total_rows = len(raw_df)
        logger.info("Read %d rows from s3://%s/%s", total_rows, bucket, key)

        # LOGIC — step 2: validate rows; split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date)
        rows_rejected = len(rejected_df)
        logger.info("Validation complete: valid=%d rejected=%d", len(valid_df), rows_rejected)

        # LOGIC — step 3: load valid rows into Aurora with idempotent upsert
        rows_inserted = position_loader.load_positions(valid_df)
        logger.info("Loaded %d rows into demo_schema.trade_positions", rows_inserted)

        # LOGIC — step 4: build summary report and manifest; write both to S3
        report_s3_key = report_builder.build_and_store_report(
            bucket=bucket,
            source_key=key,
            desk_code=desk_code,
            trade_date=trade_date,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
        )
        logger.info("Report written to s3://%s/%s", bucket, report_s3_key)

        # LOGIC — step 5: write error file only when there are rejected rows
        if rows_rejected > 0:
            error_s3_key = error_writer.write_error_file(bucket, key, rejected_df)
            logger.info("Error file written to s3://%s/%s", bucket, error_s3_key)

        # LOGIC — determine pipeline status
        if rows_rejected > 0:
            status = "PARTIAL"
        else:
            status = "SUCCESS"

        # LOGIC — step 6: write audit record
        audit_logger.write_audit_record(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
        )
        logger.info("Audit record written with status=%s", status)

        # LOGIC — step 7: publish success SNS notification
        manifest_s3_key = f"manifests/{desk_code}_{trade_date}_manifest.json"
        sns_notifier.notify_success(
            desk_code=desk_code,
            trade_date=trade_date,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            report_s3_key=report_s3_key,
            manifest_s3_key=manifest_s3_key,
            processing_timestamp_et=processing_timestamp_et,
        )
        logger.info("Success SNS notification published")

        # LOGIC — return well-formed Lambda response; statusCode 200 for partial success too
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_s3_key,   # null when no rejections
                    "report_file": report_s3_key,
                }
            ),
        }

    except Exception as exc:  # LOGIC — catch-all: write FAILED audit, publish failure SNS, re-raise
        error_message = str(exc)
        logger.exception("Unhandled exception during pipeline execution: %s", error_message)

        # LOGIC — best-effort audit write; guard against secondary failure
        try:
            audit_logger.write_audit_record(
                filename=filename or "UNKNOWN",
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=total_rows,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=error_message,
            )
        except Exception as audit_exc:  # BOILERPLATE
            logger.error("Failed to write FAILED audit record: %s", str(audit_exc))

        # LOGIC — best-effort failure SNS notification
        try:
            sns_notifier.notify_failure(
                filename=filename or "UNKNOWN",
                error_message=error_message,
                processing_timestamp_et=processing_timestamp_et,
            )
        except Exception as sns_exc:  # BOILERPLATE
            logger.error("Failed to publish failure SNS notification: %s", str(sns_exc))

        # LOGIC — re-raise so Lambda marks invocation as failed
        raise