# BOILERPLATE
import json
import logging
import os

import boto3

from timestamp_helper import format_et, now_et

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def publish_success(summary: dict) -> None:
    # LOGIC — build and publish the success SNS payload per the SNS data contract
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    message_payload = {
        "event": "POSITION_LOAD_SUCCESS",
        "filename": summary.get("filename", ""),
        "desk_code": summary.get("desk_code", ""),
        "trade_date": summary.get("trade_date", ""),
        "processing_timestamp_et": summary.get("processing_timestamp_et", ""),
        "total_rows": summary.get("total_rows", 0),
        "rows_loaded": summary.get("rows_loaded", 0),
        "rows_rejected": summary.get("rows_rejected", 0),
        "report_s3_key": summary.get("report_s3_key", ""),
        "manifest_s3_key": summary.get("manifest_s3_key", ""),
    }

    message_str = json.dumps(message_payload)

    # BOILERPLATE — boto3 SNS client; no credentials in code, uses IAM role
    sns_client = boto3.client("sns")

    logger.info(
        "Publishing success notification to SNS topic for filename=%s desk_code=%s trade_date=%s",
        summary.get("filename"),
        summary.get("desk_code"),
        summary.get("trade_date"),
    )

    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="POSITION_LOAD_SUCCESS",
    )

    logger.info(
        "SNS success notification published. MessageId=%s",
        response.get("MessageId"),
    )


def publish_failure(
    filename: str,
    error: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    # LOGIC — build and publish the failure SNS payload per the SNS data contract
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # BOILERPLATE — stamp current ET time for the failure notification
    now = now_et()
    timestamp_str = format_et(now)

    message_payload = {
        "event": "POSITION_LOAD_FAILURE",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": timestamp_str,
        "error": error,
    }

    message_str = json.dumps(message_payload)

    # BOILERPLATE — boto3 SNS client; no credentials in code, uses IAM role
    sns_client = boto3.client("sns")

    logger.info(
        "Publishing failure notification to SNS topic for filename=%s error=%s",
        filename,
        error,
    )

    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="POSITION_LOAD_FAILURE",
    )

    logger.info(
        "SNS failure notification published. MessageId=%s",
        response.get("MessageId"),
    )