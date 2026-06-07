# BOILERPLATE
import json
import logging
from datetime import datetime

import pytz

import src.config as config
import src.file_reader as file_reader
import src.validator as validator
import src.error_writer as error_writer
import src.loader as loader
import src.reporter as reporter
import src.notifier as notifier
import src.audit as audit

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_TZ_ET = pytz.timezone("America/Toronto")


def _determine_status(rows_rejected: int, rows_inserted: int, total_rows: int) -> str:
    # LOGIC — derive pipeline status string from outcome counts
    if rows_rejected == 0 and rows_inserted >= 0:
        return "SUCCESS"
    if rows_inserted > 0 and rows_rejected > 0:
        return "PARTIAL"
    if rows_inserted == 0 and rows_rejected > 0:
        return "PARTIAL"
    return "SUCCESS"


def process_file(s3_key: str, s3_client, sns_client, db_conn) -> dict:
    # BOILERPLATE — capture processing start timestamp in ET, never UTC
    processing_start: datetime = datetime.now(tz=_TZ_ET)

    # LOGIC — initialize mutable outcome variables so the except block can always reference them
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted: int = 0
    report: dict = {}
    desk_code: str = ""
    trade_date: str = ""
    error_key: str = ""

    logger.info("pipeline: starting processing for key=%s", s3_key)

    try:
        # LOGIC — step 1: read the file from S3 and extract desk_code / trade_date from filename
        raw_df, desk_code, trade_date = file_reader.read_position_file(
            s3_client, config.S3_BUCKET, s3_key
        )
        logger.info(
            "pipeline: read %d rows from s3://%s/%s desk_code=%s trade_date=%s",
            len(raw_df),
            config.S3_BUCKET,
            s3_key,
            desk_code,
            trade_date,
        )

        # LOGIC — step 2: validate rows, split into valid and rejected DataFrames
        valid_df, rejected_df = validator.validate_rows(raw_df, desk_code, trade_date)
        logger.info(
            "pipeline: validation complete valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 3: write rejected rows to S3 error prefix (no-op if empty)
        error_key = error_writer.write_error_file(
            s3_client,
            config.S3_BUCKET,
            config.S3_ERROR_PREFIX,
            desk_code,
            trade_date,
            rejected_df,
        )
        if error_key:
            logger.info("pipeline: error file written to s3://%s/%s", config.S3_BUCKET, error_key)
        else:
            logger.info("pipeline: no rejected rows — error file not written")

        # LOGIC — step 4: load valid rows into Aurora, returns count of rows actually inserted
        rows_inserted = loader.load_positions(db_conn, valid_df)
        logger.info(
            "pipeline: loader inserted %d rows (skipped %d duplicates)",
            rows_inserted,
            len(valid_df) - rows_inserted,
        )

        # LOGIC — step 5: build and write processing summary report to S3
        report = reporter.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code,
            trade_date=trade_date,
            source_s3_key=s3_key,
            processing_start=processing_start,
        )
        report_key = reporter.write_report(
            s3_client,
            config.S3_BUCKET,
            config.S3_REPORT_PREFIX,
            desk_code,
            trade_date,
            report,
        )
        logger.info("pipeline: report written to s3://%s/%s", config.S3_BUCKET, report_key)

        # LOGIC — step 6: write audit record regardless of partial/full success
        processing_end: datetime = datetime.now(tz=_TZ_ET)
        status = _determine_status(len(rejected_df), rows_inserted, len(raw_df))

        audit.write_audit_record(
            conn=db_conn,
            source_key=s3_key,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=len(raw_df),
            rows_loaded=rows_inserted,
            rows_rejected=len(rejected_df),
            rows_skipped=len(valid_df) - rows_inserted,
            error_message=None,
            processing_start=processing_start,
            processing_end=processing_end,
        )
        logger.info("pipeline: audit record written status=%s", status)

        # LOGIC — step 7: publish success SNS notification
        notifier.notify_success(sns_client, config.SNS_SUCCESS_ARN, report)
        logger.info("pipeline: success notification published to %s", config.SNS_SUCCESS_ARN)

        return report

    except Exception as exc:  # LOGIC — guaranteed audit + failure notification on any unhandled error
        processing_end = datetime.now(tz=_TZ_ET)
        error_message = str(exc)
        logger.error(
            "pipeline: unhandled exception processing key=%s error=%s",
            s3_key,
            error_message,
            exc_info=True,
        )

        # LOGIC — write audit record with FAILURE status; use safe defaults for missing counts
        total_rows = len(raw_df) if raw_df is not None else 0
        rejected_count = len(rejected_df) if rejected_df is not None else 0
        valid_count = len(valid_df) if valid_df is not None else 0
        skipped_count = max(0, valid_count - rows_inserted)

        audit.write_audit_record(
            conn=db_conn,
            source_key=s3_key,
            desk_code=desk_code,
            trade_date=trade_date,
            status="FAILURE",
            total_rows=total_rows,
            rows_loaded=rows_inserted,
            rows_rejected=rejected_count,
            rows_skipped=skipped_count,
            error_message=error_message,
            processing_start=processing_start,
            processing_end=processing_end,
        )

        # LOGIC — publish failure notification; errors in notify must not suppress the original exception
        try:
            notifier.notify_failure(
                sns_client,
                config.SNS_FAILURE_ARN,
                desk_code=desk_code,
                trade_date=trade_date,
                error_message=error_message,
                source_key=s3_key,
            )
            logger.info(
                "pipeline: failure notification published to %s", config.SNS_FAILURE_ARN
            )
        except Exception as notify_exc:
            logger.error(
                "pipeline: failed to publish failure notification error=%s", str(notify_exc)
            )

        # LOGIC — re-raise the original exception as required by the design contract
        raise