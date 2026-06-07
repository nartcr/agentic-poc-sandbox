import json
import logging
import os
from datetime import datetime

import boto3
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET_ZONE = pytz.timezone("America/Toronto")


def _get_et_timestamp() -> str:
    # LOGIC — generate ISO-8601 timestamp in Eastern Time
    return datetime.now(_ET_ZONE).isoformat()


def _build_sns_client():
    # BOILERPLATE
    return boto3.client("sns")


def notify_success(report: dict) -> None:
    # LOGIC — publish SUCCESS notification to SNS with full summary report payload
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    sns_client = _build_sns_client()

    message_payload = {
        "message_type": "SUCCESS",
        "source_file_key": report.get("source_file_key", ""),
        "desk_code": report.get("desk_code", ""),
        "trade_date": report.get("trade_date", ""),
        "processing_timestamp": report.get("processing_timestamp", _get_et_timestamp()),
        "total_rows_received": report.get("total_rows_received", 0),
        "rows_successfully_loaded": report.get("rows_successfully_loaded", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "rows_by_desk_code": report.get("rows_by_desk_code", {}),
        "min_notional_amount": report.get("min_notional_amount", None),
        "max_notional_amount": report.get("max_notional_amount", None),
        "null_rates": report.get("null_rates", {}),
        "report_s3_key": report.get("report_s3_key", ""),
    }

    message_str = json.dumps(message_payload)

    logger.info(
        "Publishing SUCCESS SNS notification for source_file_key=%s to topic=%s",
        report.get("source_file_key", ""),
        topic_arn,
    )

    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="Pipeline SUCCESS: " + report.get("source_file_key", ""),
    )

    logger.info(
        "SNS SUCCESS notification published. MessageId=%s",
        response.get("MessageId", ""),
    )


def notify_failure(
    error_type: str,
    error_message: str,
    source_file_key: str,
) -> None:
    # LOGIC — publish FAILURE notification to SNS with error details
    topic_arn = os.environ["SNS_TOPIC_ARN"]
    sns_client = _build_sns_client()

    message_payload = {
        "message_type": "FAILURE",
        "source_file_key": source_file_key,
        "processing_timestamp": _get_et_timestamp(),
        "error_type": error_type,
        "error_message": error_message,
    }

    message_str = json.dumps(message_payload)

    logger.error(
        "Publishing FAILURE SNS notification for source_file_key=%s error_type=%s to topic=%s",
        source_file_key,
        error_type,
        topic_arn,
    )

    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="Pipeline FAILURE: " + source_file_key,
    )

    logger.info(
        "SNS FAILURE notification published. MessageId=%s",
        response.get("MessageId", ""),
    )