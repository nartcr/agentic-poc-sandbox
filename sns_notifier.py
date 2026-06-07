# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

_ET = pytz.timezone("America/Toronto")


def _format_dt_et(dt: datetime) -> str:
    # LOGIC — format a timezone-aware datetime as ISO 8601 with UTC offset
    if dt.tzinfo is None:
        dt = _ET.localize(dt)
    else:
        dt = dt.astimezone(_ET)
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def notify_success(summary: dict, report_s3_key: str) -> None:
    # LOGIC — publish a POSITIONS_LOADED message to the success SNS topic
    topic_arn = os.environ["SNS_SUCCESS_ARN"]

    payload = {
        "event": "POSITIONS_LOADED",
        "filename": summary.get("filename", ""),
        "desk_code": summary.get("desk_code", ""),
        "trade_date": summary.get("trade_date", ""),
        "total_rows_received": summary.get("total_rows_received", 0),
        "rows_successfully_loaded": summary.get("rows_successfully_loaded", 0),
        "rows_rejected": summary.get("rows_rejected", 0),
        "rows_skipped_duplicate": summary.get("rows_skipped_duplicate", 0),
        "report_s3_key": report_s3_key,
        "processing_timestamp_et": summary.get("processing_timestamp_et", ""),
    }

    message_body = json.dumps(payload)

    try:
        client = boto3.client("sns")
        client.publish(
            TopicArn=topic_arn,
            Subject="POSITIONS_LOADED",
            Message=message_body,
        )
        logger.info(
            "SNS success notification published for file=%s topic=%s",
            summary.get("filename", ""),
            topic_arn,
        )
    except Exception as exc:  # LOGIC — never raise; log and continue
        logger.warning(
            "Failed to publish SNS success notification: %s", exc, exc_info=True
        )


def notify_failure(
    filename: str,
    desk_code: str | None,
    trade_date: str | None,
    error_message: str,
    timestamp_et: datetime,
) -> None:
    # LOGIC — publish a POSITIONS_FAILED message to the failure SNS topic
    topic_arn = os.environ["SNS_FAILURE_ARN"]

    payload = {
        "event": "POSITIONS_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
        "processing_timestamp_et": _format_dt_et(timestamp_et),
    }

    message_body = json.dumps(payload)

    try:
        client = boto3.client("sns")
        client.publish(
            TopicArn=topic_arn,
            Subject="POSITIONS_FAILED",
            Message=message_body,
        )
        logger.info(
            "SNS failure notification published for file=%s topic=%s",
            filename,
            topic_arn,
        )
    except Exception as exc:  # LOGIC — never raise; log and continue
        logger.warning(
            "Failed to publish SNS failure notification: %s", exc, exc_info=True
        )