# BOILERPLATE
import json
import logging
import os
from datetime import datetime
from typing import Optional

import boto3
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_ET_TZ = pytz.timezone("America/Toronto")


def _current_et_isoformat() -> str:
    # LOGIC — current timestamp as ISO 8601 string in America/Toronto timezone
    return datetime.now(_ET_TZ).isoformat()


def _get_sns_client():
    # BOILERPLATE — boto3 client; no credentials in code
    return boto3.client("sns")


def publish_success(report: dict) -> None:
    # LOGIC — publishes structured success payload to SNS success topic
    desk_code = report.get("desk_code", "")
    trade_date = report.get("trade_date", "")

    report_s3_key = (
        f"reports/{desk_code}_{trade_date}_report.json"
        if desk_code and trade_date
        else report.get("report_s3_key", "")
    )

    payload = {
        "event": "TRADE_POSITION_LOAD_SUCCESS",
        "filename": report.get("filename", ""),
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": report.get("total_rows", 0),
        "rows_inserted": report.get("rows_inserted", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "processing_timestamp_et": report.get("processing_timestamp_et", _current_et_isoformat()),
        "report_s3_key": report_s3_key,
    }

    subject = f"Trade Position Load: SUCCESS \u2014 {desk_code} {trade_date}"

    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    logger.info(
        "sns_notifier: publishing success notification to topic_arn=%r "
        "desk_code=%r trade_date=%r rows_inserted=%d",
        topic_arn,
        desk_code,
        trade_date,
        payload["rows_inserted"],
    )

    # LOGIC — publish to SNS; message is full JSON payload
    client = _get_sns_client()
    client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject=subject,
    )

    logger.info(
        "sns_notifier: success notification published for desk_code=%r trade_date=%r",
        desk_code,
        trade_date,
    )


def publish_failure(
    filename: str,
    error_message: str,
    desk_code: Optional[str],
    trade_date_str: Optional[str],
) -> None:
    # LOGIC — publishes structured failure payload to SNS failure topic
    failure_ts = _current_et_isoformat()

    payload = {
        "event": "TRADE_POSITION_LOAD_FAILURE",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "error_message": error_message,
        "failure_timestamp_et": failure_ts,
    }

    subject = f"Trade Position Load: FAILURE \u2014 {filename}"

    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    logger.error(
        "sns_notifier: publishing failure notification to topic_arn=%r "
        "filename=%r desk_code=%r error_message=%r",
        topic_arn,
        filename,
        desk_code,
        error_message,
    )

    # LOGIC — publish to SNS failure topic
    client = _get_sns_client()
    client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject=subject,
    )

    logger.info(
        "sns_notifier: failure notification published for filename=%r",
        filename,
    )