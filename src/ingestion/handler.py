# BOILERPLATE
import json
import logging
import os
import urllib.parse
from datetime import datetime

import boto3
import pytz

from src.ingestion import (
    audit,
    db,
    error_writer,
    file_reader,
    loader,
    notifier,
    reporter,
    validator,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — Eastern Time zone used for all timestamps
_ET = pytz.timezone("America/Toronto")


def _process_single_file(s3_key: str, s3_client, sns_client, conn) -> dict:
    """
    # LOGIC
    Orchestrates the full ingestion pipeline for a single S3 file:
      file_reader → validator → loader → error_writer → reporter → audit → notifier

    Returns the summary dict produced by reporter.build_summary().
    """
    bucket = os.environ["S3_BUCKET"]
    topic_arn = os.environ["SNS_TOPIC_ARN"]

    # LOGIC — capture processing start time once; reused everywhere for consistency
    processed_at = datetime.now(_ET)

    # LOGIC — Step 1: read and parse the input file from S3
    raw_df, desk_code, trade_date = file_reader.read_position_file(
        s3_client, bucket, s3_key
    )
    logger.info(
        "File read complete — rows=%d desk_code=%s trade_date=%s",
        len(raw_df),
        desk_code,
        trade_date,
    )

    # LOGIC — Step 2: validate rows
    valid_df, rejected_df = validator.validate_rows(raw_df)
    logger.info(
        "Validation complete — valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    # LOGIC — Step 3: load valid rows into the database
    rows_inserted = loader.load_positions(valid_df, conn)
    logger.info("Loader complete — rows_inserted=%d", rows_inserted)

    # LOGIC — Step 4: write rejected rows to S3 error file (only when there are rejections)
    if len(rejected_df) > 0:
        error_key = error_writer.write_error_file(
            rejected_df, s3_client, bucket, s3_key
        )
        logger.info("Error file written — key=%s", error_key)
    else:
        logger.info("No rejected rows — skipping error file write")

    # LOGIC — Step 5: build summary report
    summary = reporter.build_summary(
        source_key=s3_key,
        raw_df=raw_df,
        valid_df=valid_df,
        rejected_df=rejected_df,
        rows_inserted=rows_inserted,
        processed_at=processed_at,
    )

    # LOGIC — Step 6: write summary report JSON to S3
    processed_at_ts = processed_at.strftime("%Y%m%dT%H%M%S")
    report_key = f"reports/{desk_code}_{trade_date}_{processed_at_ts}.json"
    reporter.write_report(summary, s3_client, bucket, report_key)
    logger.info("Report written — key=%s", report_key)

    # LOGIC — Step 7: write audit record (SUCCESS)
    rows_skipped = len(valid_df) - rows_inserted
    audit.write_audit_record(
        conn=conn,
        source_file=s3_key,
        status="SUCCESS",
        total_rows=len(raw_df),
        rows_loaded=rows_inserted,
        rows_rejected=len(rejected_df),
        rows_skipped=rows_skipped,
        error_message=None,
        processed_at=processed_at,
    )
    logger.info("Audit record written — status=SUCCESS")

    # LOGIC — Step 8: publish success notification to SNS
    # Augment summary with the report S3 key for downstream consumers
    summary["report_s3_key"] = report_key
    notifier.notify_success(summary, sns_client, topic_arn)
    logger.info("SNS success notification published")

    return summary


def lambda_handler(event: dict, context: object) -> dict:
    """
    # LOGIC
    AWS Lambda entry point. Processes the first S3 record in the event.
    Returns {"statusCode": 200, "body": <summary_json>} on success.
    Returns {"statusCode": 500, "body": <error_json>} on failure.
    """
    # BOILERPLATE — initialise AWS clients once per invocation
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # LOGIC — extract bucket and key from the S3 event notification
    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        # LOGIC — S3 keys in event notifications are URL-encoded
        raw_key = record["s3"]["object"]["key"]
        s3_key = urllib.parse.unquote_plus(raw_key)
    except (KeyError, IndexError) as exc:
        logger.error("Malformed S3 event payload: %s", exc)
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Malformed event: {exc}"}),
        }

    logger.info(
        "Lambda invoked — bucket=%s key=%s", bucket_name, s3_key
    )

    # LOGIC — open a single DB connection for the full pipeline lifetime
    try:
        with db.get_connection() as conn:
            try:
                summary = _process_single_file(s3_key, s3_client, sns_client, conn)
                return {
                    "statusCode": 200,
                    "body": json.dumps(summary, default=str),
                }
            except Exception as pipeline_exc:  # noqa: BLE001
                logger.exception(
                    "Pipeline error for key=%s: %s", s3_key, pipeline_exc
                )
                error_message = f"{type(pipeline_exc).__name__}: {pipeline_exc}"
                processed_at = datetime.now(_ET)
                topic_arn = os.environ["SNS_TOPIC_ARN"]

                # LOGIC — best-effort audit write; do not let DB failure suppress SNS
                try:
                    audit.write_audit_record(
                        conn=conn,
                        source_file=s3_key,
                        status="FAILURE",
                        total_rows=0,
                        rows_loaded=0,
                        rows_rejected=0,
                        rows_skipped=0,
                        error_message=error_message,
                        processed_at=processed_at,
                    )
                    logger.info("Audit record written — status=FAILURE")
                except Exception as audit_exc:  # noqa: BLE001
                    logger.error(
                        "Failed to write FAILURE audit record: %s", audit_exc
                    )

                # LOGIC — always publish failure notification
                try:
                    notifier.notify_failure(
                        source_key=s3_key,
                        error_message=error_message,
                        sns_client=sns_client,
                        topic_arn=topic_arn,
                    )
                    logger.info("SNS failure notification published")
                except Exception as sns_exc:  # noqa: BLE001
                    logger.error(
                        "Failed to publish SNS failure notification: %s", sns_exc
                    )

                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": error_message}),
                }

    except Exception as conn_exc:  # noqa: BLE001
        # LOGIC — DB connection itself failed; no audit possible, still notify
        logger.exception(
            "DB connection error for key=%s: %s", s3_key, conn_exc
        )
        error_message = f"{type(conn_exc).__name__}: {conn_exc}"
        topic_arn = os.environ.get("SNS_TOPIC_ARN", "")

        if topic_arn:
            try:
                notifier.notify_failure(
                    source_key=s3_key,
                    error_message=error_message,
                    sns_client=sns_client,
                    topic_arn=topic_arn,
                )
            except Exception as sns_exc:  # noqa: BLE001
                logger.error(
                    "Failed to publish SNS failure notification after DB error: %s",
                    sns_exc,
                )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": error_message}),
        }