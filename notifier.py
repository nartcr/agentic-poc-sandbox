import json
import logging
import boto3
from datetime import datetime

import pytz  # BOILERPLATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")  # BOILERPLATE


def notify_success(topic_arn: str, report: dict) -> None:
    # LOGIC
    """
    Publishes to SNS topic identified by topic_arn.
    Message subject: "Trade Position Ingestion SUCCESS: {desk_code} {trade_date}"
    Message body: JSON-serialized report dict (same structure as reporter.py output).
    """
    desk_code = report.get("desk_code", "UNKNOWN")
    trade_date = report.get("trade_date", "UNKNOWN")
    subject = f"Trade Position Ingestion SUCCESS: {desk_code} {trade_date}"

    try:
        message_body = json.dumps(report, default=str)
    except (TypeError, ValueError) as exc:
        logger.error("notify_success: failed to serialize report to JSON: %s", exc)
        raise

    client = boto3.client("sns")  # BOILERPLATE
    try:
        response = client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message_body,
        )
        logger.info(
            "notify_success: published to %s, MessageId=%s",
            topic_arn,
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error("notify_success: SNS publish failed for topic %s: %s", topic_arn, exc)
        raise


def notify_failure(
    topic_arn: str,
    s3_key: str,
    error_message: str,
    processing_timestamp: datetime,
) -> None:
    # LOGIC
    """
    Publishes to SNS topic identified by topic_arn.
    Message subject: "Trade Position Ingestion FAILURE: {s3_key}"
    Message body: JSON with keys:
      file_name, error_message, processing_timestamp_et (ISO 8601 ET)
    """
    subject = f"Trade Position Ingestion FAILURE: {s3_key}"

    # LOGIC — ensure timestamp carries ET tzinfo before formatting
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    else:
        processing_timestamp = processing_timestamp.astimezone(_ET)

    processing_timestamp_et = processing_timestamp.isoformat()

    body = {
        "file_name": s3_key,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    try:
        message_body = json.dumps(body, default=str)
    except (TypeError, ValueError) as exc:
        logger.error("notify_failure: failed to serialize failure body to JSON: %s", exc)
        raise

    client = boto3.client("sns")  # BOILERPLATE
    try:
        response = client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message_body,
        )
        logger.info(
            "notify_failure: published to %s, MessageId=%s",
            topic_arn,
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error("notify_failure: SNS publish failed for topic %s: %s", topic_arn, exc)
        raise