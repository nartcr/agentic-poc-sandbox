# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def publish_success(summary: dict) -> None:
    # LOGIC
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    payload = {
        "event": "POSITION_LOAD_SUCCESS",
        "filename": summary["filename"],
        "desk_code": summary["desk_code"],
        "trade_date": summary["trade_date"],
        "processing_timestamp_et": summary["processing_timestamp_et"],
        "total_rows": summary["total_rows"],
        "rows_inserted": summary["rows_inserted"],
        "rows_rejected": summary["rows_rejected"],
        "rows_skipped_duplicate": summary["rows_skipped_duplicate"],
        "report_s3_key": summary["report_s3_key"],
    }

    # BOILERPLATE
    client = boto3.client("sns")

    # LOGIC
    response = client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject="POSITION_LOAD_SUCCESS",
    )

    message_id = response["MessageId"]
    logger.info(
        "SNS success notification published. MessageId=%s topic=%s filename=%s",
        message_id,
        topic_arn,
        summary["filename"],
    )


def publish_failure(
    filename: str,
    error_message: str,
    desk_code: str | None,
    trade_date: str | None,
) -> None:
    # LOGIC
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    et_tz = pytz.timezone("America/Toronto")
    processing_timestamp_et = datetime.now(et_tz).isoformat()

    payload = {
        "event": "POSITION_LOAD_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp_et": processing_timestamp_et,
        "error_message": error_message,
    }

    # BOILERPLATE
    client = boto3.client("sns")

    # LOGIC
    response = client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject="POSITION_LOAD_FAILED",
    )

    message_id = response["MessageId"]
    logger.info(
        "SNS failure notification published. MessageId=%s topic=%s filename=%s",
        message_id,
        topic_arn,
        filename,
    )