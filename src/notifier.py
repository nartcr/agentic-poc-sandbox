# BOILERPLATE
import json
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)


class _PayloadEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects in SNS payloads."""  # BOILERPLATE

    def default(self, obj):  # LOGIC
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def notify_success(sns_client, topic_arn: str, summary: dict) -> None:
    """
    Publish a POSITION_LOAD_SUCCESS message to the success SNS topic.
    summary is the dict returned by reporter.build_summary.
    Does not mutate the input summary dict.
    """  # LOGIC

    payload = dict(summary)
    payload["event"] = "POSITION_LOAD_SUCCESS"

    message_body = json.dumps(payload, cls=_PayloadEncoder)

    logger.info(
        "Publishing success notification to %s for desk_code=%s trade_date=%s",
        topic_arn,
        summary.get("desk_code"),
        summary.get("trade_date"),
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
    )

    logger.info("Success notification published.")


def notify_failure(
    sns_client,
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    s3_key: str,
    error_detail: str,
) -> None:
    """
    Publish a POSITION_LOAD_FAILURE message to the failure SNS topic.
    Stamps failure_timestamp_et at call time using ET timezone.
    """  # LOGIC

    et_tz = pytz.timezone("America/Toronto")  # BOILERPLATE
    failure_ts = datetime.now(et_tz)

    payload = {
        "event": "POSITION_LOAD_FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "s3_key": s3_key,
        "error_detail": error_detail,
        "failure_timestamp_et": failure_ts.isoformat(),
    }

    message_body = json.dumps(payload, cls=_PayloadEncoder)

    logger.info(
        "Publishing failure notification to %s for desk_code=%s trade_date=%s s3_key=%s",
        topic_arn,
        desk_code,
        trade_date,
        s3_key,
    )

    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
    )

    logger.info("Failure notification published.")