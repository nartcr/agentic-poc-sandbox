# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/Toronto")


def _get_sns_client():
    # BOILERPLATE
    return boto3.client("sns")


def _current_timestamp_et_iso() -> str:
    # LOGIC — fallback timestamp when caller does not supply one
    return datetime.now(_ET).isoformat()


def notify_success(report: dict) -> None:
    # LOGIC — publish a structured success notification to the success SNS topic
    topic_arn = os.environ["SNS_SUCCESS_ARN"]

    message_body = {
        "event": "POSITION_LOAD_SUCCESS",
        "source_file": report.get("source_file"),
        "desk_code": report.get("desk_code"),
        "trade_date": report.get("trade_date"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
        "total_rows_received": report.get("total_rows_received"),
        "rows_successfully_loaded": report.get("rows_successfully_loaded"),
        "rows_rejected": report.get("rows_rejected"),
    }

    message_str = json.dumps(message_body, default=str)

    logger.info(
        "Publishing success notification to SNS topic for source_file=%s desk_code=%s trade_date=%s",
        message_body["source_file"],
        message_body["desk_code"],
        message_body["trade_date"],
    )

    try:
        client = _get_sns_client()
        response = client.publish(
            TopicArn=topic_arn,
            Message=message_str,
            Subject="Position Load Success",
        )
        logger.info(
            "SNS success notification published. MessageId=%s",
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error(
            "Failed to publish success SNS notification for source_file=%s: %s",
            message_body.get("source_file"),
            exc,
        )
        raise


def notify_failure(
    filename: str,
    error: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    # LOGIC — publish a structured failure notification to the failure SNS topic
    topic_arn = os.environ["SNS_FAILURE_ARN"]

    processing_timestamp_et = _current_timestamp_et_iso()

    message_body = {
        "event": "POSITION_LOAD_FAILURE",
        "source_file": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error": error,
        "processing_timestamp_et": processing_timestamp_et,
    }

    message_str = json.dumps(message_body, default=str)

    logger.info(
        "Publishing failure notification to SNS topic for source_file=%s desk_code=%s trade_date=%s",
        filename,
        desk_code,
        trade_date,
    )

    try:
        client = _get_sns_client()
        response = client.publish(
            TopicArn=topic_arn,
            Message=message_str,
            Subject="Position Load Failure",
        )
        logger.info(
            "SNS failure notification published. MessageId=%s",
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error(
            "Failed to publish failure SNS notification for source_file=%s: %s",
            filename,
            exc,
        )
        raise