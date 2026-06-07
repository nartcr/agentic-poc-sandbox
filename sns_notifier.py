# BOILERPLATE
import json
import logging
import os

import boto3

from timestamp_helper import to_et_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — SNS client instantiated once per Lambda container
_sns_client = None


def _get_sns_client():
    # BOILERPLATE
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


def notify_success(report: dict) -> None:
    # LOGIC — publish success notification to the success SNS topic
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    # LOGIC — build the SNS payload per the DATA CONTRACTS success message schema
    payload = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "filename": report.get("filename"),
        "desk_code": report.get("desk_code"),
        "trade_date": report.get("trade_date"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
        "total_rows_received": report.get("total_rows_received"),
        "rows_successfully_loaded": report.get("rows_successfully_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate"),
        "report_s3_key": report.get("report_s3_key"),
        "manifest_s3_key": report.get("manifest_s3_key"),
    }

    message_body = json.dumps(payload)

    logger.info(
        "Publishing success notification to SNS topic %s for file %s",
        topic_arn,
        report.get("filename"),
    )

    client = _get_sns_client()
    client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject="Trade Positions Load Success",
    )

    logger.info("Success SNS notification published.")


def notify_failure(filename: str, error_message: str, processing_timestamp_et) -> None:
    # LOGIC — publish failure notification to the failure SNS topic
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — build the SNS payload per the DATA CONTRACTS failure message schema
    payload = {
        "event_type": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "processing_timestamp_et": to_et_string(processing_timestamp_et),
        "error_message": error_message,
    }

    message_body = json.dumps(payload)

    logger.info(
        "Publishing failure notification to SNS topic %s for file %s",
        topic_arn,
        filename,
    )

    client = _get_sns_client()
    client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject="Trade Positions Load Failure",
    )

    logger.info("Failure SNS notification published.")