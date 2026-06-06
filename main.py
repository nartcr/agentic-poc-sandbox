# BOILERPLATE
import logging
from datetime import datetime

import boto3
import pytz

import auditor
import config
import error_writer
import file_reader
import loader
import notifier
import reporter
import secrets as secrets_module
import validator

# BOILERPLATE — module-level logger; callers configure handlers
logger = logging.getLogger(__name__)


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — record invocation start for performance logging (TAC-6)
    invocation_start = datetime.now(pytz.timezone("America/Toronto"))
    logger.info("Pipeline invocation started at %s", invocation_start.strftime("%Y-%m-%dT%H:%M:%S%z"))

    # LOGIC — load all config from environment variables once per invocation
    cfg = config.load_config()

    # LOGIC — retrieve DB credentials once per invocation; reused across all files (no re-fetch)
    db_credentials = secrets_module.get_db_credentials(cfg.db_secret_id)

    # BOILERPLATE — instantiate AWS clients once per invocation
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # LOGIC — discover all pending position CSV files under the input prefix
    pending_keys = file_reader.list_pending_files(s3_client, cfg.s3_bucket, cfg.s3_input_prefix)
    logger.info("Discovered %d pending file(s) to process", len(pending_keys))

    files_processed = 0
    files_failed = 0

    for file_key in pending_keys:
        # LOGIC — capture per-file processing timestamp in ET at the start of each file's work
        processing_ts = datetime.now(pytz.timezone("America/Toronto"))
        desk_code = None
        trade_date = None

        try:
            # LOGIC — step b: download CSV and extract desk_code / trade_date from filename
            raw_df, desk_code, trade_date = file_reader.read_csv_from_s3(
                s3_client, cfg.s3_bucket, file_key
            )
            logger.info(
                "Read file key=%s desk_code=%s trade_date=%s rows=%d",
                file_key,
                desk_code,
                trade_date,
                len(raw_df),
            )

            # LOGIC — step c: validate rows; produces valid_df and rejected_df
            valid_df, rejected_df = validator.validate_rows(raw_df, desk_code, trade_date)
            logger.info(
                "Validation complete for key=%s valid=%d rejected=%d",
                file_key,
                len(valid_df),
                len(rejected_df),
            )

            # LOGIC — step d: load valid rows to DB only if any exist; otherwise rows_inserted = 0
            if len(valid_df) > 0:
                rows_inserted = loader.load_positions(valid_df, db_credentials)
            else:
                rows_inserted = 0
                logger.info("No valid rows to load for key=%s", file_key)

            # LOGIC — step e: always write error file (even when rejected_df is empty)
            error_s3_key = error_writer.write_error_file(
                s3_client,
                rejected_df,
                cfg.s3_bucket,
                cfg.s3_error_prefix,
                desk_code,
                trade_date,
            )
            logger.info("Error file written to s3_key=%s", error_s3_key)

            # LOGIC — step f: build and write summary report
            report = reporter.build_report(
                raw_df,
                valid_df,
                rejected_df,
                rows_inserted,
                desk_code,
                trade_date,
                processing_ts,
            )
            report_s3_key = reporter.write_report(
                s3_client,
                report,
                cfg.s3_bucket,
                cfg.s3_report_prefix,
                desk_code,
                trade_date,
            )
            logger.info("Report written to s3_key=%s", report_s3_key)

            # LOGIC — step g: publish success SNS notification with full report stats
            # Inject extra fields required by SNS success message schema before notifying
            report["status"] = "SUCCESS"
            report["report_s3_key"] = report_s3_key
            notifier.notify_success(sns_client, cfg.sns_success_topic_arn, report)
            logger.info(
                "Success notification published for desk_code=%s trade_date=%s",
                desk_code,
                trade_date,
            )

            # LOGIC — step h: write SUCCESS audit record
            auditor.write_audit_record(
                db_credentials,
                {
                    "_audit_table": cfg.audit_table,
                    "file_key": file_key,
                    "desk_code": desk_code,
                    "trade_date": trade_date,
                    "status": "SUCCESS",
                    "total_rows": len(raw_df),
                    "rows_loaded": rows_inserted,
                    "rows_rejected": len(rejected_df),
                    "error_summary": None,
                },
            )

            files_processed += 1
            logger.info("Successfully processed file key=%s", file_key)

        except Exception as exc:
            # LOGIC — step i: per-file failure isolation; notify and audit then continue to next file
            files_failed += 1
            error_details = str(exc)
            logger.exception("Unhandled exception processing file key=%s", file_key)

            # LOGIC — use fallback values for desk_code / trade_date if extraction failed
            safe_desk_code = desk_code if desk_code is not None else "UNKNOWN"
            safe_trade_date = trade_date if trade_date is not None else "UNKNOWN"

            try:
                notifier.notify_failure(
                    sns_client,
                    cfg.sns_failure_topic_arn,
                    safe_desk_code,
                    safe_trade_date,
                    error_details,
                    processing_ts,
                )
            except Exception:
                logger.exception(
                    "Failed to publish failure notification for key=%s", file_key
                )

            try:
                auditor.write_audit_record(
                    db_credentials,
                    {
                        "_audit_table": cfg.audit_table,
                        "file_key": file_key,
                        "desk_code": safe_desk_code,
                        "trade_date": safe_trade_date,
                        "status": "FAILURE",
                        "total_rows": 0,
                        "rows_loaded": 0,
                        "rows_rejected": 0,
                        "error_summary": error_details,
                    },
                )
            except Exception:
                logger.exception(
                    "Failed to write failure audit record for key=%s", file_key
                )

    # BOILERPLATE — log total elapsed time for TAC-6 performance validation
    invocation_end = datetime.now(pytz.timezone("America/Toronto"))
    elapsed_seconds = (invocation_end - invocation_start).total_seconds()
    logger.info(
        "Pipeline invocation complete at %s — elapsed=%.2fs files_processed=%d files_failed=%d",
        invocation_end.strftime("%Y-%m-%dT%H:%M:%S%z"),
        elapsed_seconds,
        files_processed,
        files_failed,
    )

    return {"files_processed": files_processed, "files_failed": files_failed}