# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/Toronto")


def notify_success(  # LOGIC
    topic_arn: str,
    report: dict,
) -> None:
    """
    Publishes a success SNS message after a successful process_file() call.
    Message is a JSON string with event_type='POSITIONS_LOADED' plus key report fields.
    SNS publish failures are logged at ERROR level and not re-raised.
    """
    desk_code = report.get("desk_code", "")
    trade_date = report.get("trade_date", "")

    # LOGIC — success message payload per DATA CONTRACTS SNS section
    message_payload = {
        "event_type": "POSITIONS_LOADED",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "rows_inserted": report.get("rows_inserted"),
        "rows_rejected": report.get("rows_rejected"),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate"),
        "report_s3_key": report.get("report_s3_key"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
    }

    subject = f"POSITIONS_LOADED: {desk_code} {trade_date}"  # LOGIC — SNS subject per DATA CONTRACTS

    _publish_to_sns(topic_arn=topic_arn, message_payload=message_payload, subject=subject)


def notify_failure(  # LOGIC
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    error_message: str,
    processing_timestamp: datetime,
) -> None:
    """
    Publishes a failure SNS message when process_file() raises an exception.
    SNS publish failures are logged at ERROR level and not re-raised.
    """
    # LOGIC — ensure processing_timestamp is in ET
    if processing_timestamp.tzinfo is None:
        processing_timestamp = _ET.localize(processing_timestamp)
    else:
        processing_timestamp = processing_timestamp.astimezone(_ET)

    processing_timestamp_et_str = processing_timestamp.isoformat()

    # LOGIC — failure message payload per DATA CONTRACTS SNS section
    message_payload = {
        "event_type": "POSITIONS_LOAD_FAILED",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et_str,
    }

    subject = f"POSITIONS_LOAD_FAILED: {desk_code} {trade_date}"  # LOGIC — SNS subject per DATA CONTRACTS

    _publish_to_sns(topic_arn=topic_arn, message_payload=message_payload, subject=subject)


def _publish_to_sns(  # LOGIC
    topic_arn: str,
    message_payload: dict,
    subject: str,
) -> None:
    """
    Internal helper: serializes payload to JSON and publishes to SNS.
    Catches all exceptions, logs at ERROR level, does not re-raise.
    """
    try:
        message_str = json.dumps(message_payload, ensure_ascii=False)  # LOGIC

        # BOILERPLATE — SNS client
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message_str,
            Subject=subject,
        )
        logger.info(
            "SNS notification published: subject=%r topic_arn=%s",
            subject,
            topic_arn,
        )
    except Exception as exc:  # LOGIC — notification failure must not mask primary result
        logger.error(
            "Failed to publish SNS notification to %s (subject=%r): %s",
            topic_arn,
            subject,
            exc,
        )