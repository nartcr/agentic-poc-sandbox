# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — Eastern Time zone constant
_ET = pytz.timezone("America/Toronto")


def _et_now_iso() -> str:
    # LOGIC — single source of truth for ET timestamp strings used in SNS payloads
    return datetime.now(_ET).isoformat()


def notify_success(summary: dict) -> None:
    # LOGIC — read topic ARN from environment; never hardcoded
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    # LOGIC — build success payload matching the Data Contracts SNS schema exactly
    payload = {
        "event": "TRADE_POSITION_LOAD_SUCCESS",
        "filename": summary.get("filename"),
        "desk_code": summary.get("desk_code"),
        "trade_date": summary.get("trade_date"),
        "total_rows": summary.get("total_rows"),
        "rows_loaded": summary.get("rows_loaded"),
        "rows_rejected": summary.get("rows_rejected"),
        "report_s3_key": summary.get("report_s3_key"),
        "manifest_s3_key": summary.get("manifest_s3_key"),
        "processing_timestamp_et": summary.get(
            "processing_timestamp_et", _et_now_iso()
        ),
    }

    message_body = json.dumps(payload)
    logger.info(
        "sns_notifier: publishing success notification for filename=%r to topic=%r",
        payload["filename"],
        topic_arn,
    )

    try:
        # BOILERPLATE — boto3 SNS client; credentials from Lambda execution role
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=topic_arn,
            Message=message_body,
            Subject="Trade Position Load Success",
        )
        logger.info(
            "sns_notifier: success notification published for filename=%r",
            payload["filename"],
        )
    except Exception:
        # LOGIC — re-raise so pipeline_handler can catch and record it
        logger.error(
            "sns_notifier: failed to publish success notification for filename=%r",
            payload["filename"],
            exc_info=True,
        )
        raise


def notify_failure(error_info: dict) -> None:
    # LOGIC — read failure topic ARN from environment; never hardcoded
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — build failure payload matching the Data Contracts SNS schema exactly
    payload = {
        "event": "TRADE_POSITION_LOAD_FAILURE",
        "filename": error_info.get("filename"),
        "desk_code": error_info.get("desk_code"),
        "trade_date": error_info.get("trade_date"),
        "error_message": error_info.get("error_message"),
        "processing_timestamp_et": error_info.get(
            "processing_timestamp_et", _et_now_iso()
        ),
    }

    message_body = json.dumps(payload)
    logger.info(
        "sns_notifier: publishing failure notification for filename=%r to topic=%r",
        payload["filename"],
        topic_arn,
    )

    try:
        # BOILERPLATE — boto3 SNS client; credentials from Lambda execution role
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=topic_arn,
            Message=message_body,
            Subject="Trade Position Load Failure",
        )
        logger.info(
            "sns_notifier: failure notification published for filename=%r",
            payload["filename"],
        )
    except Exception:
        # LOGIC — re-raise so pipeline_handler can catch and record it
        logger.error(
            "sns_notifier: failed to publish failure notification for filename=%r",
            payload["filename"],
            exc_info=True,
        )
        raise