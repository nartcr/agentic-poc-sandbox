# BOILERPLATE
import json
import logging
import boto3
import pytz
from datetime import datetime

from config import config

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/Toronto")


def notify_success(topic_arn: str, report: dict) -> None:
    # LOGIC: publish a TRADE_POSITION_LOAD_SUCCESS message to SNS
    message_payload = {
        "event_type": "TRADE_POSITION_LOAD_SUCCESS",
        "desk_code": report.get("desk_code", ""),
        "trade_date": report.get("trade_date", ""),
        "source_file": report.get("source_file", ""),
        "rows_inserted": report.get("rows_inserted", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "report_s3_key": report.get("report_s3_key", ""),
        "processing_timestamp_et": report.get("processing_timestamp_et", ""),
    }

    message_str = json.dumps(message_payload)
    subject = "TRADE_POSITION_LOAD_SUCCESS"

    # BOILERPLATE
    sns_client = boto3.client("sns", region_name=config.aws_region)

    logger.info(
        "Publishing success notification to SNS topic: %s (desk=%s trade_date=%s rows_inserted=%d)",
        topic_arn,
        message_payload["desk_code"],
        message_payload["trade_date"],
        message_payload["rows_inserted"],
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.info("SNS success notification published.")


def notify_failure(topic_arn: str, error_details: dict) -> None:
    # LOGIC: publish a TRADE_POSITION_LOAD_FAILURE message to SNS
    processing_timestamp_et = datetime.now(ET).isoformat()

    message_payload = {
        "event_type": "TRADE_POSITION_LOAD_FAILURE",
        "source_file": error_details.get("source_file", "unknown"),
        "error_type": error_details.get("error_type", ""),
        "error_message": error_details.get("error_message", ""),
        "processing_timestamp_et": error_details.get(
            "processing_timestamp_et", processing_timestamp_et
        ),
    }

    message_str = json.dumps(message_payload)
    subject = "TRADE_POSITION_LOAD_FAILURE"

    # BOILERPLATE
    sns_client = boto3.client("sns", region_name=config.aws_region)

    logger.info(
        "Publishing failure notification to SNS topic: %s (source_file=%s error_type=%s)",
        topic_arn,
        message_payload["source_file"],
        message_payload["error_type"],
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.info("SNS failure notification published.")