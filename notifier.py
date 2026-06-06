import json
import logging
import os

import pytz
from datetime import datetime

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def notify_success(report: dict, sns_client) -> None:
    # LOGIC — publish success notification to SNS success topic
    topic_arn = os.environ["SNS_TOPIC_ARN_SUCCESS"]

    desk_code = report.get("desk_code", "")
    trade_date = report.get("trade_date", "")
    subject = f"Trade Position Load Complete: {desk_code} {trade_date}"

    # LOGIC — build message body per SNS success schema in data contracts
    message_body = {
        "event_type": "TRADE_POSITION_LOAD_COMPLETE",
        "source_file": report.get("source_file", ""),
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": report.get("total_rows_received"),
        "rows_loaded": report.get("rows_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "processing_timestamp_et": report.get("processing_timestamp_et", ""),
        "min_notional_amount": report.get("min_notional_amount"),
        "max_notional_amount": report.get("max_notional_amount"),
        "null_rates": report.get("null_rates", {}),
        "desk_code_counts": report.get("desk_code_counts", {}),
    }

    message_str = json.dumps(message_body)

    logger.info(
        "Publishing success notification to SNS topic. desk_code=%s trade_date=%s rows_loaded=%s",
        desk_code,
        trade_date,
        message_body.get("rows_loaded"),
    )

    # LOGIC — publish to SNS; let boto3 exceptions propagate to orchestrator
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject[:100],  # SNS subject max 100 chars
    )

    logger.info(
        "Success SNS message published. MessageId=%s",
        response.get("MessageId"),
    )


def notify_failure(
    file_name: str,
    error_message: str,
    error_type: str,
    processing_timestamp_et: str,
    sns_client,
) -> None:
    # LOGIC — publish failure notification to SNS failure topic
    topic_arn = os.environ["SNS_TOPIC_ARN_FAILURE"]

    subject = f"Trade Position Load FAILED: {file_name}"

    # LOGIC — build message body per SNS failure schema in data contracts
    message_body = {
        "event_type": "TRADE_POSITION_LOAD_FAILED",
        "file_name": file_name,
        "error_type": error_type,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    message_str = json.dumps(message_body)

    logger.info(
        "Publishing failure notification to SNS topic. file_name=%s error_type=%s",
        file_name,
        error_type,
    )

    # LOGIC — publish to SNS; let boto3 exceptions propagate to orchestrator
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject[:100],  # SNS subject max 100 chars
    )

    logger.info(
        "Failure SNS message published. MessageId=%s",
        response.get("MessageId"),
    )