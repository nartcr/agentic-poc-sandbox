# BOILERPLATE
import json
import logging
import os

import boto3

import time_utils

logger = logging.getLogger(__name__)

# BOILERPLATE — module-level SNS client, created once per cold start
_sns_client = boto3.client("sns")


def notify_success(summary: dict) -> None:
    # LOGIC — build the success message payload per Data Contracts SNS schema
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    message_payload = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "filename": summary["filename"],
        "desk_code": summary["desk_code"],
        "trade_date": summary["trade_date"],
        "processing_timestamp_et": summary["processing_timestamp_et"],
        "total_rows": summary["total_rows"],
        "rows_loaded": summary["rows_loaded"],
        "rows_rejected": summary["rows_rejected"],
        "report_s3_key": summary["report_s3_key"],
    }

    message_str = json.dumps(message_payload)

    logger.info(
        "Publishing success notification to SNS topic %s for file %s",
        topic_arn,
        summary.get("filename"),
    )

    # LOGIC — publish to SNS success topic
    _sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITIONS_LOADED",
    )

    logger.info("Success SNS notification published.")


def notify_failure(
    filename: str,
    error_message: str,
    processing_timestamp_et,
) -> None:
    # LOGIC — build the failure message payload per Data Contracts SNS schema
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    message_payload = {
        "event_type": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "error_message": error_message,
        "processing_timestamp_et": time_utils.format_et(processing_timestamp_et),
    }

    message_str = json.dumps(message_payload)

    logger.info(
        "Publishing failure notification to SNS topic %s for file %s",
        topic_arn,
        filename,
    )

    # LOGIC — publish to SNS failure topic
    _sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITIONS_FAILED",
    )

    logger.info("Failure SNS notification published.")