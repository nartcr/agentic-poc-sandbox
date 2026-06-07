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
def publish_success(report: dict) -> None:
    """Publishes the processing summary report to the SNS success topic."""
    # LOGIC — read topic ARN from environment; never hardcoded
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    desk_code = report.get("desk_code", "UNKNOWN")
    trade_date = report.get("trade_date", "UNKNOWN")

    subject = f"TradePositionIngestion:SUCCESS:{desk_code}:{trade_date}"

    # LOGIC — message body is full report dict serialized to JSON
    message_body = json.dumps(report, default=str)

    # BOILERPLATE — SNS client created at call time
    sns_client = boto3.client("sns")

    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message_body,
    )

    logger.info(
        "Success notification published for desk_code=%s trade_date=%s "
        "MessageId=%s",
        desk_code,
        trade_date,
        response.get("MessageId"),
    )


# LOGIC
def publish_failure(
    desk_code: str,
    trade_date: str,
    error_message: str,
    s3_key: str | None,
) -> None:
    """Publishes a failure notification to the SNS failure topic."""
    # LOGIC — read topic ARN from environment; never hardcoded
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    subject = f"TradePositionIngestion:FAILURE:{desk_code}:{trade_date}"

    # LOGIC — timestamp in ET per TAC-7
    et_zone = pytz.timezone("America/Toronto")
    timestamp = datetime.now(et_zone).isoformat()

    # LOGIC — failure message body per data contract schema
    failure_payload = {
        "event_type": "TradePositionIngestion:FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
        "s3_key": s3_key,
        "timestamp": timestamp,
    }

    message_body = json.dumps(failure_payload, default=str)

    # BOILERPLATE — SNS client created at call time
    sns_client = boto3.client("sns")

    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message_body,
    )

    logger.info(
        "Failure notification published for desk_code=%s trade_date=%s "
        "error=%s MessageId=%s",
        desk_code,
        trade_date,
        error_message,
        response.get("MessageId"),
    )