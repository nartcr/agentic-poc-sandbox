# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC
_ET = pytz.timezone("America/Toronto")


def _et_now_iso() -> str:
    """Return current Eastern Time as an ISO 8601 string."""  # LOGIC
    return datetime.now(_ET).strftime("%Y-%m-%dT%H:%M:%S%z")


def publish_success(report: dict, report_s3_key: str) -> None:
    """Publish a success notification to the SNS success topic."""  # LOGIC
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]  # BOILERPLATE

    # LOGIC — build message payload exactly per data contract
    message = {
        "event": "TRADE_POSITION_LOAD_SUCCESS",
        "filename": report.get("filename"),
        "desk_code": report.get("desk_code"),
        "trade_date": report.get("trade_date"),
        "total_rows": report.get("total_rows"),
        "rows_inserted": report.get("rows_inserted"),
        "rows_rejected": report.get("rows_rejected"),
        "report_s3_key": report_s3_key,
        "processing_timestamp_et": report.get("processing_timestamp_et", _et_now_iso()),
    }

    message_str = json.dumps(message, default=str)  # LOGIC

    # BOILERPLATE — create client at call time; avoids stale credentials in warm containers
    sns_client = boto3.client("sns")

    response = sns_client.publish(  # LOGIC
        TopicArn=topic_arn,
        Subject="Trade Position Load SUCCESS",
        Message=message_str,
    )

    message_id = response.get("MessageId")  # LOGIC
    logger.info(
        "SNS success notification published. MessageId=%s TopicArn=%s filename=%s",
        message_id,
        topic_arn,
        message.get("filename"),
    )


def publish_failure(
    filename: str,
    error_message: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    """Publish a failure notification to the SNS failure topic."""  # LOGIC
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]  # BOILERPLATE

    # LOGIC — build message payload exactly per data contract
    message = {
        "event": "TRADE_POSITION_LOAD_FAILURE",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
        "processing_timestamp_et": _et_now_iso(),
    }

    message_str = json.dumps(message, default=str)  # LOGIC

    # BOILERPLATE — create client at call time
    sns_client = boto3.client("sns")

    response = sns_client.publish(  # LOGIC
        TopicArn=topic_arn,
        Subject="Trade Position Load FAILURE",
        Message=message_str,
    )

    message_id = response.get("MessageId")  # LOGIC
    logger.info(
        "SNS failure notification published. MessageId=%s TopicArn=%s filename=%s",
        message_id,
        topic_arn,
        filename,
    )