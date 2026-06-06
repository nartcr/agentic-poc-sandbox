# BOILERPLATE
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def notify_success(sns_client, topic_arn: str, report: dict) -> None:
    # LOGIC — build success payload by copying report and injecting event_type
    payload = {"event_type": "POSITION_LOAD_SUCCESS"}
    payload.update(report)

    message_body = json.dumps(payload)

    logger.info(
        "Publishing success notification to SNS topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        report.get("desk_code"),
        report.get("trade_date"),
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
    )

    logger.info(
        "Success notification published to SNS topic %s",
        topic_arn,
    )


def notify_failure(
    sns_client,
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    s3_key: str,
    error_message: str,
    processed_at: datetime,
) -> None:
    # LOGIC — build failure payload per the defined failure message schema
    payload = {
        "event_type": "POSITION_LOAD_FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_s3_key": s3_key,
        "error_message": error_message,
        "processed_at": processed_at.isoformat(),
    }

    message_body = json.dumps(payload)

    logger.info(
        "Publishing failure notification to SNS topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        desk_code,
        trade_date,
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
    )

    logger.info(
        "Failure notification published to SNS topic %s",
        topic_arn,
    )