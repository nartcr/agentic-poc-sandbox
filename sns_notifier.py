import json
import logging
import os
from decimal import Decimal
import datetime

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# BOILERPLATE — custom JSON encoder to handle Decimal and date types
# that may be present in summary_dict from report_builder
class _SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # LOGIC — convert Decimal to float for JSON serialisation
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            # LOGIC — ISO 8601 string representation for date/datetime
            return obj.isoformat()
        return super().default(obj)


def _get_sns_client():
    # BOILERPLATE — create SNS client using ambient Lambda IAM credentials
    return boto3.client("sns")


def send_success(summary_dict: dict) -> None:
    """
    Publish a success notification to SNS_SUCCESS_TOPIC_ARN.

    Called after a SUCCESS or PARTIAL pipeline outcome.
    Message body is the full summary_dict serialised as JSON.
    Subject includes desk_code and trade_date for quick identification.
    """
    # LOGIC — extract identifiers for the subject line; fall back gracefully
    desk_code = summary_dict.get("desk_code", "UNKNOWN")
    trade_date = summary_dict.get("trade_date", "UNKNOWN")

    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]
    subject = f"Trade Position Load Success: {desk_code} {trade_date}"

    # LOGIC — serialise using safe encoder so Decimal/date values don't raise
    message_body = json.dumps(summary_dict, cls=_SafeEncoder, indent=2)

    logger.info(
        "Publishing success notification to SNS topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        desk_code,
        trade_date,
    )

    client = _get_sns_client()
    response = client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject=subject,
    )

    # BOILERPLATE — log message ID returned by SNS for traceability
    message_id = response.get("MessageId", "N/A")
    logger.info(
        "SNS success notification published. MessageId=%s desk_code=%s trade_date=%s",
        message_id,
        desk_code,
        trade_date,
    )


def send_failure(error_details: dict) -> None:
    """
    Publish a failure notification to SNS_FAILURE_TOPIC_ARN.

    Called on unhandled exception or complete load failure (FAILED status).
    Message body is the error_details dict serialised as JSON.
    Subject includes desk_code and trade_date for quick identification.
    """
    # LOGIC — extract identifiers for the subject line; fall back gracefully
    desk_code = error_details.get("desk_code") or "UNKNOWN"
    trade_date = error_details.get("trade_date") or "UNKNOWN"

    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]
    subject = f"Trade Position Load FAILED: {desk_code} {trade_date}"

    # LOGIC — serialise error details; safe encoder handles any edge-case types
    message_body = json.dumps(error_details, cls=_SafeEncoder, indent=2)

    logger.error(
        "Publishing failure notification to SNS topic %s for desk_code=%s trade_date=%s",
        topic_arn,
        desk_code,
        trade_date,
    )

    client = _get_sns_client()
    response = client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject=subject,
    )

    # BOILERPLATE — log message ID returned by SNS for traceability
    message_id = response.get("MessageId", "N/A")
    logger.warning(
        "SNS failure notification published. MessageId=%s desk_code=%s trade_date=%s",
        message_id,
        desk_code,
        trade_date,
    )