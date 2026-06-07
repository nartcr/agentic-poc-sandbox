import json
import logging
import re
from datetime import datetime

import pytz

import audit
import error_writer
import file_reader
import loader
import notifier
import reporter
import validator
from config import Config
from exceptions import AuditWriteError
from secrets import get_db_credentials

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — pattern for expected incoming S3 key
_KEY_PATTERN = re.compile(
    r"^incoming/(?P<desk_code>[^_]+(?:_[^_]+)*)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)


def _parse_s3_key(key: str):
    """Extract desk_code and trade_date from the S3 object key.

    Returns (desk_code, trade_date) or raises ValueError.
    """
    # LOGIC — validate key against expected naming convention
    match = _KEY_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            "'incoming/{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    return match.group("desk_code"), match.group("trade_date")


def lambda_handler(event: dict, context: object) -> dict:
    """AWS Lambda entry point. Orchestrates the full trade position pipeline."""

    # BOILERPLATE — load config once at handler start
    cfg = Config()

    et_tz = pytz.timezone("America/Toronto")

    # LOGIC — extract S3 event metadata
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    # S3 URL-encodes the key; decode spaces at minimum
    key = record["object"]["key"].replace("+", " ")
    from urllib.parse import unquote_plus
    key = unquote_plus(record["object"]["key"])

    source_key = key  # preserve for error reporting

    desk_code = "unknown"
    trade_date = "unknown"

    try:
        # LOGIC — step 2: validate key pattern and extract metadata
        desk_code, trade_date = _parse_s3_key(key)

        # LOGIC — step 3: retrieve DB credentials at runtime
        credentials = get_db_credentials(cfg.db_secret_id)

        # LOGIC — step 4: download and parse CSV from S3
        raw_df = file_reader.download_and_parse(bucket, key)

        # LOGIC — step 5: validate rows, split into valid and rejected
        valid_df, rejected_df = validator.validate_rows(raw_df)

        # LOGIC — step 6: load valid rows into Aurora (idempotent upsert)
        rows_inserted = loader.load_positions(valid_df, credentials)

        # LOGIC — step 7: write error file if any rows were rejected
        if len(rejected_df) > 0:
            error_writer.write_error_file(
                rejected_df,
                cfg.s3_bucket,
                desk_code,
                trade_date,
            )

        # LOGIC — step 8: build and write summary report to S3
        report = reporter.build_and_write_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            bucket=cfg.s3_bucket,
            desk_code=desk_code,
            trade_date=trade_date,
            source_key=source_key,
        )

        # LOGIC — step 9: notify downstream on success
        notifier.notify_success(cfg.sns_topic_arn, report)

        # LOGIC — step 10: write audit record with SUCCESS outcome
        processing_ts = datetime.now(et_tz)
        try:
            audit.write_audit_record(
                credentials=credentials,
                source_file=source_key,
                desk_code=desk_code,
                trade_date=trade_date,
                outcome="SUCCESS",
                total_rows=len(raw_df),
                rows_inserted=rows_inserted,
                rows_rejected=len(rejected_df),
                error_message=None,
                processing_timestamp_et=processing_ts,
            )
        except AuditWriteError as audit_exc:
            # LOGIC — audit failure is logged but does not fail the pipeline
            logger.error(
                "Audit write failed on SUCCESS path: %s", audit_exc, exc_info=True
            )

        # LOGIC — step 11: return 200 with report payload
        logger.info(
            "Pipeline completed successfully: desk_code=%s trade_date=%s "
            "rows_inserted=%d rows_rejected=%d",
            desk_code,
            trade_date,
            rows_inserted,
            len(rejected_df),
        )
        return {"statusCode": 200, "body": json.dumps(report, default=str)}

    except Exception as exc:
        # LOGIC — failure path: notify and audit before re-raising
        processing_ts = datetime.now(et_tz)
        logger.error(
            "Pipeline failed: source_file=%s error=%s",
            source_key,
            exc,
            exc_info=True,
        )

        error_details = {
            "source_file": source_key,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "processing_timestamp_et": processing_ts.isoformat(),
        }

        # LOGIC — best-effort failure notification; never suppress original exception
        try:
            notifier.notify_failure(cfg.sns_topic_arn, error_details)
        except Exception as notify_exc:
            logger.error(
                "Failed to send failure notification: %s", notify_exc, exc_info=True
            )

        # LOGIC — best-effort audit write on failure path
        try:
            # credentials may not have been retrieved if failure was early
            if "credentials" in dir():
                creds_for_audit = credentials  # noqa: F821
            else:
                creds_for_audit = get_db_credentials(cfg.db_secret_id)

            audit.write_audit_record(
                credentials=creds_for_audit,
                source_file=source_key,
                desk_code=desk_code,
                trade_date=trade_date,
                outcome="FAILURE",
                total_rows=len(raw_df) if "raw_df" in dir() else 0,
                rows_inserted=rows_inserted if "rows_inserted" in dir() else 0,
                rows_rejected=len(rejected_df) if "rejected_df" in dir() else 0,
                error_message=str(exc),
                processing_timestamp_et=processing_ts,
            )
        except Exception as audit_exc:
            logger.error(
                "Audit write failed on FAILURE path: %s", audit_exc, exc_info=True
            )

        # LOGIC — re-raise so Lambda marks invocation as failed and triggers retry
        raise