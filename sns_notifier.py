# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


def _et_now_iso() -> str:
    # LOGIC — current Eastern Time as ISO-8601 string with UTC offset
    return datetime.now(_ET).isoformat()


def notify_success(report: dict) -> None:
    # LOGIC — publish success notification to SNS success topic
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    message_payload = {
        "event": "TRADE_POSITIONS_LOADED",
        "desk_code": report.get("desk_code"),
        "trade_date": report.get("trade_date"),
        "filename": report.get("filename"),
        "total_rows": report.get("total_rows"),
        "rows_loaded": report.get("rows_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
        "report_s3_key": report.get("report_s3_key"),
        "manifest_s3_key": report.get("manifest_s3_key"),
    }

    message_str = json.dumps(message_payload)

    # BOILERPLATE
    sns_client = boto3.client("sns")

    logger.info(
        "Publishing success notification to SNS topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        message_payload.get("desk_code"),
        message_payload.get("trade_date"),
    )

    # LOGIC — publish to SNS success topic
    sns_client.publish(
        TopicArn=topic_arn,
        Subject="TRADE_POSITIONS_LOADED",
        Message=message_str,
    )

    logger.info("Success SNS notification published successfully.")


def notify_failure(
    filename: str,
    error_detail: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    # LOGIC — publish failure notification to SNS failure topic
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    message_payload = {
        "event": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_detail": error_detail,
        "processing_timestamp_et": _et_now_iso(),
    }

    message_str = json.dumps(message_payload)

    # BOILERPLATE
    sns_client = boto3.client("sns")

    logger.info(
        "Publishing failure notification to SNS topic %s for filename=%s",
        topic_arn,
        filename,
    )

    # LOGIC — publish to SNS failure topic
    sns_client.publish(
        TopicArn=topic_arn,
        Subject="TRADE_POSITIONS_FAILED",
        Message=message_str,
    )

    logger.info("Failure SNS notification published successfully.")