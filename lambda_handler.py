# BOILERPLATE
import json
import logging
import os
import re
import traceback

import pytz

from datetime import datetime

import file_reader
import row_validator
import db_loader
import report_builder
import error_writer
import audit_logger
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — ET timezone constant
ET = pytz.timezone("America/Toronto")

# LOGIC — regex to parse the structured filename: {desk_code}_{trade_date}_positions.csv
# trade_date is YYYY-MM-DD (contains hyphens, not underscores)
# desk_code may contain letters, digits, and hyphens but NOT underscores
# Pattern: everything up to the last occurrence of _YYYY-MM-DD_positions.csv
_FILENAME_RE = re.compile(
    r"^(?P<desk_code>[^_]+(?:_[^_]+)*)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_filename(key: str):
    # LOGIC — strip any leading prefix (e.g. "incoming/") to get the bare filename
    filename = key.split("/")[-1]
    match = _FILENAME_RE.match(filename)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            "'<desk_code>_<YYYY-MM-DD>_positions.csv'"
        )
    return filename, match.group("desk_code"), match.group("trade_date")


def _determine_status(rows_inserted: int, rows_rejected: int, total_rows: int) -> str:
    # LOGIC — derive pipeline status per the design spec
    if rows_inserted == 0 and total_rows > 0:
        return "FAILED"
    if rows_rejected > 0 and rows_inserted > 0:
        return "PARTIAL"
    if rows_rejected == 0 and rows_inserted > 0:
        return "SUCCESS"
    # Edge case: empty file — treat as FAILED
    if total_rows == 0:
        return "FAILED"
    return "FAILED"


def handler(event: dict, context) -> dict:
    # BOILERPLATE — extract S3 trigger details
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info("Lambda triggered for s3://%s/%s", bucket, key)

    desk_code = None
    trade_date = None
    filename = key.split("/")[-1]

    try:
        # LOGIC — parse filename to extract desk_code and trade_date
        filename, desk_code, trade_date = _parse_filename(key)
        logger.info("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date)

        # LOGIC — step 1: read raw CSV from S3
        raw_df = file_reader.read_csv_from_s3(bucket, key)

        # LOGIC — step 2: validate rows, split into valid and rejected sets
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        total_rows = len(valid_df) + len(rejected_df)
        logger.info(
            "Validation complete: total=%d valid=%d rejected=%d",
            total_rows, len(valid_df), len(rejected_df),
        )

        # LOGIC — step 3: load valid rows into DB; get actual inserted count
        rows_inserted = db_loader.load_positions(valid_df)
        rows_rejected = len(rejected_df)
        logger.info("DB load complete: rows_inserted=%d", rows_inserted)

        # LOGIC — step 4: build and save report + manifest
        summary_dict = report_builder.build_and_save_report(
            valid_df=valid_df,
            rejected_df=rejected_df,
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            rows_inserted=rows_inserted,
        )
        report_file = summary_dict.get("report_s3_key")

        # LOGIC — step 5: write error file if any rows were rejected
        error_file = None
        if rows_rejected > 0:
            error_file = error_writer.write_error_file(
                rejected_df=rejected_df,
                bucket=bucket,
                desk_code=desk_code,
                trade_date=trade_date,
            )
            logger.info("Error file written to S3: %s", error_file)

        # LOGIC — step 6: determine pipeline status
        status = _determine_status(rows_inserted, rows_rejected, total_rows)
        logger.info("Pipeline status: %s", status)

        # LOGIC — step 7: write audit record
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

        # LOGIC — step 8: send SNS notification (success or partial → success topic)
        if status in ("SUCCESS", "PARTIAL"):
            sns_notifier.send_success(summary_dict)
        else:
            # FAILED status with no exception (e.g. empty file, zero inserts)
            processing_ts = datetime.now(ET).isoformat()
            sns_notifier.send_failure(
                {
                    "filename": filename,
                    "desk_code": desk_code,
                    "trade_date": trade_date,
                    "error_message": f"Pipeline status FAILED: rows_inserted={rows_inserted}, total_rows={total_rows}",
                    "processing_timestamp_et": processing_ts,
                }
            )

        # BOILERPLATE — return well-formed Lambda response
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "rows_inserted": rows_inserted,
                    "rows_rejected": rows_rejected,
                    "error_file": error_file,
                    "report_file": report_file,
                }
            ),
        }

    except Exception as exc:  # LOGIC — unhandled exception path
        logger.error("Unhandled exception processing %s: %s", key, exc, exc_info=True)
        error_message = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
        processing_ts = datetime.now(ET).isoformat()

        # LOGIC — always write audit record on failure, even if desk_code/trade_date are unknown
        try:
            audit_logger.write_audit_record(
                filename=filename,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILED",
                total_rows=0,
                rows_inserted=0,
                rows_rejected=0,
                error_message=error_message,
            )
        except Exception as audit_exc:
            logger.error("Failed to write audit record: %s", audit_exc, exc_info=True)

        # LOGIC — always send failure SNS on unhandled exception
        try:
            sns_notifier.send_failure(
                {
                    "filename": filename,
                    "desk_code": desk_code,
                    "trade_date": trade_date,
                    "error_message": error_message,
                    "processing_timestamp_et": processing_ts,
                }
            )
        except Exception as sns_exc:
            logger.error("Failed to send failure SNS: %s", sns_exc, exc_info=True)

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "rows_inserted": 0,
                    "rows_rejected": 0,
                    "error_file": None,
                    "report_file": None,
                    "error_message": str(exc),
                }
            ),
        }