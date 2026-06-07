# BOILERPLATE
import json
import logging
import os
import re
from datetime import datetime

import pytz

# BOILERPLATE — peer module imports (all exist per approved design)
import file_reader
import row_validator
import db_loader
import report_writer
import error_writer
import audit_logger
import sns_notifier

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — ET timezone, used for all timestamps in this pipeline
_ET = pytz.timezone("America/Toronto")

# LOGIC — filename regex: desk_code may contain letters/digits/hyphens but NOT underscores
# Pattern: <desk_code>_<YYYY-MM-DD>_positions.csv
# The desk_code portion is everything before the first _YYYY-MM-DD_ segment.
_FILENAME_RE = re.compile(
    r"^(?:.+/)?(?P<desk_code>[^/]+?)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _extract_s3_key(event: dict) -> tuple:
    # LOGIC — pull bucket and key from standard S3 event notification structure
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as exc:
        raise ValueError(f"Malformed S3 event payload: {exc}") from exc
    logger.info("Extracted S3 event: bucket=%s key=%s", bucket, key)
    return bucket, key


def _parse_filename(key: str) -> tuple:
    # LOGIC — use regex to extract desk_code and trade_date from the S3 key
    # Handles keys like incoming/DESK01_2026-06-15_positions.csv
    match = _FILENAME_RE.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            r"<desk_code>_<YYYY-MM-DD>_positions.csv"
        )
    desk_code = match.group("desk_code")
    trade_date_str = match.group("trade_date")
    logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date_str)
    return desk_code, trade_date_str


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — capture processing timestamp once at entry; all downstream modules use this
    processing_ts_et = datetime.now(_ET)

    # LOGIC — state variables; populated as pipeline stages succeed
    bucket = None
    key = None
    desk_code = None
    trade_date_str = None
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    error_key = None
    report_key = None
    audit_status = "FAILURE"
    error_message = None

    try:
        # LOGIC — Stage 1: extract S3 coordinates from event
        bucket, key = _extract_s3_key(event)

        # LOGIC — Stage 2: parse filename to get desk_code and trade_date
        desk_code, trade_date_str = _parse_filename(key)

        # LOGIC — Stage 3: read CSV from S3 into raw DataFrame (all columns as str)
        logger.info("Reading CSV from s3://%s/%s", bucket, key)
        raw_df = file_reader.read_csv_from_s3(bucket, key)
        logger.info("Read %d rows from CSV", len(raw_df))

        # LOGIC — Stage 4: validate rows; split into valid and rejected sets
        logger.info("Validating rows for desk_code=%s trade_date=%s", desk_code, trade_date_str)
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date_str)
        logger.info(
            "Validation complete: valid=%d rejected=%d", len(valid_df), len(rejected_df)
        )

        # LOGIC — Stage 5: write rejected rows to error file in S3 (always, even if empty)
        s3_bucket = os.environ["S3_BUCKET"]
        error_key = error_writer.write_error_file(rejected_df, s3_bucket, desk_code, trade_date_str)
        logger.info("Error file written to s3://%s/%s", s3_bucket, error_key)

        # LOGIC — Stage 6: load valid rows into Aurora; returns count of actually inserted rows
        if len(valid_df) > 0:
            rows_inserted = db_loader.load_positions(valid_df)
        else:
            rows_inserted = 0
        logger.info(
            "DB load complete: rows_inserted=%d skipped=%d",
            rows_inserted,
            len(valid_df) - rows_inserted,
        )

        # LOGIC — Stage 7: build and upload summary report + manifest
        report = report_writer.build_report(
            filename=key,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            processing_ts_et=processing_ts_et,
        )
        report_key = report_writer.upload_report(report, s3_bucket, desk_code, trade_date_str)
        manifest_key = report_writer.write_manifest(s3_bucket, desk_code, trade_date_str, report_key)
        logger.info(
            "Report written to s3://%s/%s; manifest at s3://%s/%s",
            s3_bucket, report_key, s3_bucket, manifest_key,
        )

        # LOGIC — determine audit status: PARTIAL if any rows rejected, SUCCESS if all valid
        if len(rejected_df) > 0 and rows_inserted > 0:
            audit_status = "PARTIAL"
        elif len(rejected_df) > 0 and rows_inserted == 0:
            # All rows rejected — still a partial/failure of data quality but pipeline ran
            audit_status = "PARTIAL"
        else:
            audit_status = "SUCCESS"

        # LOGIC — Stage 8: write audit record
        from datetime import date as _date
        _trade_date_obj = _date.fromisoformat(trade_date_str) if trade_date_str else None
        audit_id = audit_logger.write_audit_record(
            filename=key,
            desk_code=desk_code,
            trade_date=_trade_date_obj,
            status=audit_status,
            total_rows=len(raw_df),
            rows_inserted=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
            processing_timestamp_et=processing_ts_et,
        )
        logger.info("Audit record written: audit_id=%s status=%s", audit_id, audit_status)

        # LOGIC — Stage 9: publish success SNS notification
        sns_notifier.notify_success(report)
        logger.info("Success SNS notification published")

        # LOGIC — build structured response body (parsed by QA tests)
        response_body = json.dumps(
            {
                "rows_inserted": rows_inserted,
                "rows_rejected": len(rejected_df),
                "error_file": error_key,
                "report_file": report_key,
            }
        )
        return {"statusCode": 200, "body": response_body}

    except Exception as exc:  # noqa: BLE001
        # LOGIC — capture error details for audit and SNS failure notification
        error_message = f"{type(exc).__name__}: {exc}"
        logger.exception("Pipeline failed for key=%s: %s", key, error_message)

        # LOGIC — attempt audit record on failure (best-effort; do not re-raise if audit itself fails)
        try:
            from datetime import date as _date
            _trade_date_obj = _date.fromisoformat(trade_date_str) if trade_date_str else None
            audit_logger.write_audit_record(
                filename=key if key else "unknown",
                desk_code=desk_code,
                trade_date=_trade_date_obj,
                status="FAILURE",
                total_rows=len(raw_df) if raw_df is not None else 0,
                rows_inserted=rows_inserted,
                rows_rejected=len(rejected_df) if rejected_df is not None else 0,
                error_message=error_message[:5000],
                processing_timestamp_et=processing_ts_et,
            )
        except Exception as audit_exc:  # noqa: BLE001
            logger.error("Failed to write audit record on failure path: %s", audit_exc)

        # LOGIC — publish failure SNS notification (best-effort)
        try:
            sns_notifier.notify_failure(
                filename=key if key else "unknown",
                error=error_message,
                processing_ts_et=processing_ts_et,
            )
        except Exception as sns_exc:  # noqa: BLE001
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        return {"statusCode": 500, "body": error_message}