# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# BOILERPLATE — custom JSON encoder to handle Decimal and datetime objects in report dicts
class _ReportEncoder(json.JSONEncoder):
    def default(self, obj):
        # LOGIC
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _get_sns_client():
    # BOILERPLATE
    return boto3.client("sns")


def _current_timestamp_et() -> str:
    # LOGIC — generate processing timestamp in America/Toronto as ISO 8601 string
    tz_et = pytz.timezone("America/Toronto")
    return datetime.now(tz_et).isoformat()


def notify_success(report: dict) -> None:
    """
    Publish a TRADE_POSITION_LOAD_SUCCESS message to the SNS success topic.
    The report dict must contain all fields required by the SNS success message contract.
    """
    # BOILERPLATE
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    # LOGIC — build the structured success message matching the DATA CONTRACTS SNS schema
    message_payload = {
        "event": "TRADE_POSITION_LOAD_SUCCESS",
        "desk_code": report.get("desk_code"),
        "trade_date": report.get("trade_date"),
        "filename": report.get("filename"),
        "total_rows": report.get("total_rows"),
        "rows_loaded": report.get("rows_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate"),
        "report_key": report.get("report_key"),
        "manifest_key": report.get("manifest_key"),
        "processing_timestamp_et": report.get("processing_timestamp_et"),
    }

    # LOGIC — serialize to JSON using custom encoder to handle Decimal/datetime edge cases
    message_str = json.dumps(message_payload, cls=_ReportEncoder)

    sns_client = _get_sns_client()

    logger.info(
        "Publishing success notification to SNS topic. desk_code=%s trade_date=%s filename=%s",
        message_payload.get("desk_code"),
        message_payload.get("trade_date"),
        message_payload.get("filename"),
    )

    # LOGIC — publish to the success topic
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITION_LOAD_SUCCESS",
    )

    logger.info(
        "SNS success notification published. MessageId=%s",
        response.get("MessageId"),
    )


def notify_failure(
    filename: str,
    error_message: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    """
    Publish a TRADE_POSITION_LOAD_FAILURE message to the SNS failure topic.
    Called on any unhandled exception in the pipeline.
    """
    # BOILERPLATE
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — capture ET timestamp at point of failure notification
    processing_timestamp_et = _current_timestamp_et()

    # LOGIC — build the structured failure message matching the DATA CONTRACTS SNS schema
    message_payload = {
        "event": "TRADE_POSITION_LOAD_FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "filename": filename,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    # LOGIC — serialize to JSON
    message_str = json.dumps(message_payload)

    sns_client = _get_sns_client()

    logger.info(
        "Publishing failure notification to SNS topic. filename=%s desk_code=%s trade_date=%s",
        filename,
        desk_code,
        trade_date,
    )

    # LOGIC — publish to the failure topic
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITION_LOAD_FAILURE",
    )

    logger.info(
        "SNS failure notification published. MessageId=%s",
        response.get("MessageId"),
    )