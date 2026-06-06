# BOILERPLATE
import json
import logging
import os
import time
from datetime import datetime
from urllib.parse import unquote_plus

import boto3
import pytz

import audit
import error_writer
import loader
import notifier
import reader
import reporter
import secrets as app_secrets
import validator

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE


def _now_et() -> str:
    # LOGIC — produce ISO 8601 timestamp in ET
    return datetime.now(ET).isoformat()


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.
    Orchestrates: download → validate → load → report → notify.
    Writes an audit row at start and updates it at completion.
    """
    # BOILERPLATE — measure full pipeline duration per TAC-6
    start_time = time.monotonic()

    # BOILERPLATE — build AWS clients once per invocation
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # LOGIC — extract bucket and key from S3 event notification
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    raw_key = record["s3"]["object"]["key"]
    # S3 event keys may be URL-encoded
    key = unquote_plus(raw_key)
    file_name = os.path.basename(key)

    logger.info("Pipeline started for file=%s bucket=%s key=%s", file_name, bucket, key)

    # BOILERPLATE — resolve secrets once; shared across audit + loader
    db_secret_id = os.environ["DB_SECRET_ID"]
    secrets = app_secrets.get_db_credentials(db_secret_id)

    # LOGIC — open audit record before any processing
    audit_id = audit.start_audit(
        file_name=file_name,
        source_file_key=key,
        secrets=secrets,
    )
    logger.info("Audit record created audit_id=%d", audit_id)

    processing_timestamp_et = _now_et()
    rows_received = 0
    rows_loaded = 0
    rows_rejected = 0

    try:
        # LOGIC — step 1: download and parse CSV from S3
        raw_df, rows_received, desk_code, trade_date = reader.download_and_parse(
            bucket=bucket,
            key=key,
        )
        logger.info(
            "File parsed rows_received=%d desk_code=%s trade_date=%s",
            rows_received,
            desk_code,
            trade_date,
        )

        # LOGIC — step 2: validate rows; split into valid / rejected
        valid_df, rejected_df = validator.validate(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — step 3: load valid rows into Aurora
        rows_loaded = loader.load_records(
            valid_df=valid_df,
            source_file=key,
            secrets=secrets,
        )
        logger.info("Load complete rows_inserted=%d", rows_loaded)

        # LOGIC — step 4: write rejection error file to S3 (if any rejections)
        error_key = error_writer.write_error_file(
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            s3_client=s3_client,
        )
        if error_key:
            logger.info("Error file written to s3_key=%s", error_key)

        # LOGIC — step 5: generate and write summary report to S3
        report = reporter.generate_report(
            valid_df=valid_df,
            rejected_df=rejected_df,
            raw_df=raw_df,
            rows_received=rows_received,
            rows_loaded=rows_loaded,
            source_file=key,
            desk_code=desk_code,
            trade_date=trade_date,
            s3_client=s3_client,
        )
        logger.info("Report generated and written to S3")

        # LOGIC — step 6: publish success SNS notification
        notifier.notify_success(
            report=report,
            sns_client=sns_client,
        )
        logger.info("Success notification published")

        # LOGIC — close audit record with SUCCESS
        audit.complete_audit(
            audit_id=audit_id,
            status="SUCCESS",
            rows_received=rows_received,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            error_message=None,
            secrets=secrets,
        )

        elapsed = time.monotonic() - start_time
        logger.info("Pipeline complete elapsed_seconds=%.3f", elapsed)

        # LOGIC — build result dict returned to Lambda runtime
        result = {
            "file_name": file_name,
            "status": "SUCCESS",
            "rows_received": rows_received,
            "rows_loaded": rows_loaded,
            "rows_rejected": rows_rejected,
            "processing_timestamp_et": processing_timestamp_et,
        }
        return result

    except Exception as exc:  # LOGIC — top-level failure handler
        elapsed = time.monotonic() - start_time
        logger.exception(
            "Pipeline failed for file=%s elapsed_seconds=%.3f error=%s",
            file_name,
            elapsed,
            str(exc),
        )

        error_type = type(exc).__name__
        error_message = str(exc)
        failure_timestamp_et = _now_et()

        # LOGIC — attempt failure SNS notification; best-effort, do not mask original error
        try:
            notifier.notify_failure(
                file_name=file_name,
                error_message=error_message,
                error_type=error_type,
                processing_timestamp_et=failure_timestamp_et,
                sns_client=sns_client,
            )
        except Exception as notify_exc:  # BOILERPLATE — guard against notifier errors
            logger.error("Failed to send failure notification: %s", str(notify_exc))

        # LOGIC — mark audit record as FAILURE
        try:
            audit.complete_audit(
                audit_id=audit_id,
                status="FAILURE",
                rows_received=rows_received,
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
                error_message=error_message,
                secrets=secrets,
            )
        except Exception as audit_exc:  # BOILERPLATE — guard against audit errors
            logger.error("Failed to update audit record: %s", str(audit_exc))

        # LOGIC — re-raise so Lambda marks the invocation as failed
        raise