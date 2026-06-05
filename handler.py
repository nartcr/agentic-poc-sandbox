import json
import logging
import os
import urllib.parse
from datetime import datetime

import pytz

import auditor
import config as config_module
import error_writer
import file_reader
import loader
import notifier
import reporter
import secrets as secrets_module
import validator

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def lambda_handler(event: dict, context: object) -> dict:
    """
    Lambda entry point. Orchestrates the full pipeline for a single S3 file event.
    Guarantees a failure SNS notification is published even when unrecoverable errors occur.
    """
    # BOILERPLATE — load config once at the top of the invocation
    cfg = config_module.Config(
        S3_BUCKET=os.environ["S3_BUCKET"],
        S3_INPUT_PREFIX=os.environ["S3_INPUT_PREFIX"],
        S3_REPORTS_PREFIX=os.environ["S3_REPORTS_PREFIX"],
        S3_ERRORS_PREFIX=os.environ["S3_ERRORS_PREFIX"],
        DB_SECRET_ID=os.environ["DB_SECRET_ID"],
        SNS_SUCCESS_TOPIC_ARN=os.environ["SNS_SUCCESS_TOPIC_ARN"],
        SNS_FAILURE_TOPIC_ARN=os.environ["SNS_FAILURE_TOPIC_ARN"],
        AUDIT_TABLE=os.environ.get("AUDIT_TABLE", "app.pipeline_audit"),
        TZ=os.environ.get("TZ", "America/Toronto"),
    )

    # LOGIC — step 1: parse S3 trigger event
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    # URL-decode key in case S3 encodes spaces/special chars
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    logger.info("Lambda triggered: bucket=%s key=%s", bucket, key)

    # LOGIC — capture source_file early so failure handler can reference it
    source_file = key.split("/")[-1]

    # LOGIC — used in failure handler scope; declared before try block
    credentials = None

    try:
        # LOGIC — step 3: fetch DB credentials from Secrets Manager
        credentials = secrets_module.get_db_credentials(cfg.DB_SECRET_ID)

        # LOGIC — step 4: read CSV from S3
        raw_df, source_file = file_reader.read_csv_from_s3(bucket, key)
        logger.info("Read %d rows from %s", len(raw_df), source_file)

        # LOGIC — step 5: validate rows
        valid_df, rejected_df = validator.validate_rows(raw_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 6: load valid rows into Aurora
        rows_inserted = loader.load_trades(valid_df, credentials, source_file)
        logger.info("Rows inserted into daily_trades: %d", rows_inserted)

        # LOGIC — step 7: write error file if there are rejected rows
        error_file_key = None
        if len(rejected_df) > 0:
            error_file_key = error_writer.write_error_file(
                rejected_df, bucket, key, cfg.S3_ERRORS_PREFIX
            )
            logger.info("Error file written to: %s", error_file_key)

        # LOGIC — step 8: capture load timestamp in ET
        load_timestamp = datetime.now(_ET)

        # LOGIC — step 9: build and write report
        report = reporter.build_report(
            source_file=source_file,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            load_timestamp=load_timestamp,
            error_file_key=error_file_key,
        )
        report_key = reporter.write_report(
            report=report,
            bucket=bucket,
            source_key=key,
            reports_prefix=cfg.S3_REPORTS_PREFIX,
        )
        logger.info("Report written to: %s", report_key)

        # LOGIC — step 10: publish success SNS notification
        notifier.publish_success(report, cfg.SNS_SUCCESS_TOPIC_ARN)
        logger.info("Success SNS notification published")

        # LOGIC — step 11: determine outcome and write audit record
        outcome = "SUCCESS" if len(rejected_df) == 0 else "PARTIAL"
        operator_identity = os.environ.get("OPERATOR_IDENTITY", "lambda")

        # LOGIC — extract trade_date and desk_code from the report for audit
        trade_date_str = report.get("trade_date", "")
        desk_code_str = report.get("desk_code", "")

        auditor.write_audit_record(
            source_file=source_file,
            trade_date=trade_date_str,
            desk_code=desk_code_str,
            outcome=outcome,
            total_rows=len(raw_df),
            rows_loaded=rows_inserted,
            rows_rejected=len(rejected_df),
            error_message=None,
            report_key=report_key,
            error_file_key=error_file_key,
            processed_at=load_timestamp,
            operator_identity=operator_identity,
            credentials=credentials,
        )
        logger.info("Audit record written: outcome=%s", outcome)

        # LOGIC — step 13: return success response
        return {"statusCode": 200, "body": json.dumps(report)}

    except Exception as exc:
        # LOGIC — step 12: on any uncaught exception, publish failure SNS,
        # write FAILURE audit record, then re-raise to signal Lambda failure
        logger.error(
            "Pipeline failed for key=%s error=%s", key, str(exc), exc_info=True
        )

        error_message = str(exc)
        failure_timestamp = datetime.now(_ET)
        operator_identity = os.environ.get("OPERATOR_IDENTITY", "lambda")

        # LOGIC — attempt SNS failure notification; log but do not suppress
        try:
            notifier.publish_failure(
                source_file=source_file,
                error_message=error_message,
                topic_arn=cfg.SNS_FAILURE_TOPIC_ARN,
            )
        except Exception as sns_exc:
            logger.error("Failed to publish failure SNS notification: %s", sns_exc)

        # LOGIC — attempt FAILURE audit record; log but do not suppress
        if credentials is not None:
            try:
                auditor.write_audit_record(
                    source_file=source_file,
                    trade_date="",
                    desk_code="",
                    outcome="FAILURE",
                    total_rows=0,
                    rows_loaded=0,
                    rows_rejected=0,
                    error_message=error_message,
                    report_key=None,
                    error_file_key=None,
                    processed_at=failure_timestamp,
                    operator_identity=operator_identity,
                    credentials=credentials,
                )
            except Exception as audit_exc:
                logger.error("Failed to write FAILURE audit record: %s", audit_exc)

        # LOGIC — re-raise so Lambda marks this invocation as failed
        raise