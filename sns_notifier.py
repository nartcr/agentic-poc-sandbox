# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — lazy SNS client (created once per Lambda container)
_sns_client = None


def _get_sns_client():
    # BOILERPLATE
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


class _DatetimeEncoder(json.JSONEncoder):
    # LOGIC — serialise datetime objects to ISO-8601 with tz offset
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def notify_success(report_dict: dict, report_s3_key: str) -> None:
    """Publish a success notification to the SNS success topic."""
    # LOGIC — build success message from report_dict fields
    et_tz = pytz.timezone("America/Toronto")

    processing_ts = report_dict.get("processing_timestamp_et", "")
    # LOGIC — if it's already a string (ISO-8601) pass it through; if datetime, convert
    if isinstance(processing_ts, datetime):
        if processing_ts.tzinfo is None:
            processing_ts = et_tz.localize(processing_ts)
        processing_ts = processing_ts.isoformat()

    message = {
        "event": "TRADE_POSITIONS_LOADED",
        "filename": report_dict["filename"],
        "desk_code": report_dict["desk_code"],
        "trade_date": report_dict["trade_date"],
        "processing_timestamp_et": processing_ts,
        "total_rows_received": report_dict["total_rows_received"],
        "rows_successfully_loaded": report_dict["rows_successfully_loaded"],
        "rows_rejected": report_dict["rows_rejected"],
        "report_s3_key": report_s3_key,
    }

    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]
    message_body = json.dumps(message, cls=_DatetimeEncoder)

    logger.info(
        "Publishing success notification to SNS topic %s for file %s",
        topic_arn,
        report_dict["filename"],
    )

    _get_sns_client().publish(
        TopicArn=topic_arn,
        Subject="TRADE_POSITIONS_LOADED",
        Message=message_body,
    )

    logger.info(
        "Success notification published for file %s",
        report_dict["filename"],
    )


def notify_failure(
    filename: str,
    desk_code: str | None,
    trade_date_str: str | None,
    error_message: str,
    processing_timestamp_et: datetime,
) -> None:
    """Publish a failure notification to the SNS failure topic."""
    # LOGIC — build failure message
    et_tz = pytz.timezone("America/Toronto")

    if processing_timestamp_et.tzinfo is None:
        processing_timestamp_et = et_tz.localize(processing_timestamp_et)

    ts_str = processing_timestamp_et.isoformat()

    message = {
        "event": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "processing_timestamp_et": ts_str,
        "error_message": error_message,
    }

    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]
    message_body = json.dumps(message, cls=_DatetimeEncoder)

    logger.info(
        "Publishing failure notification to SNS topic %s for file %s",
        topic_arn,
        filename,
    )

    _get_sns_client().publish(
        TopicArn=topic_arn,
        Subject="TRADE_POSITIONS_FAILED",
        Message=message_body,
    )

    logger.info(
        "Failure notification published for file %s",
        filename,
    )