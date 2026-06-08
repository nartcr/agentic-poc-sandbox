# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_sns_client():
    # BOILERPLATE — boto3 client factory; kept in its own function for easy mocking in tests
    return boto3.client("sns")


def notify_success(report: dict) -> None:
    # LOGIC — build the success SNS message from the report dict per the data contract
    desk_code = report.get("desk_code", "")
    trade_date = report.get("trade_date", "")

    # LOGIC — manifest key is always predictable (no timestamp), per manifest path contract
    manifest_key = f"manifests/{desk_code}_{trade_date}_manifest.json"

    message_payload = {
        "event": "TRADE_POSITION_INGESTION_SUCCESS",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "filename": report.get("filename", ""),
        "total_rows": report.get("total_rows"),
        "rows_loaded": report.get("rows_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
        "report_key": report.get("report_key"),
        "manifest_key": manifest_key,
    }

    subject = f"Trade Position Ingestion Success: {desk_code} {trade_date}"
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    logger.info(
        "Publishing success SNS notification to topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — publish to SNS using boto3
    sns_client = _get_sns_client()
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message_payload),
            Subject=subject,
        )
        logger.info(
            "Success SNS notification published. MessageId=%s",
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error("Failed to publish success SNS notification: %s", exc)
        raise


def notify_failure(
    filename: str,
    error_message: str,
    desk_code: Optional[str],
    trade_date: Optional[str],
    processing_ts: datetime,
) -> None:
    # LOGIC — build the failure SNS message per the data contract
    processing_ts_str = (
        processing_ts.isoformat() if processing_ts is not None else None
    )

    message_payload = {
        "event": "TRADE_POSITION_INGESTION_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
        "processing_timestamp_et": processing_ts_str,
    }

    subject = f"Trade Position Ingestion FAILED: {filename}"
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    logger.info(
        "Publishing failure SNS notification to topic %s for filename=%s",
        topic_arn,
        filename,
    )

    # BOILERPLATE — publish to SNS using boto3
    sns_client = _get_sns_client()
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message_payload),
            Subject=subject,
        )
        logger.info(
            "Failure SNS notification published. MessageId=%s",
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error("Failed to publish failure SNS notification: %s", exc)
        raise