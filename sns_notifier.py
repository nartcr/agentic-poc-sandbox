# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET_TZ = pytz.timezone("America/Toronto")


def _get_et_timestamp() -> str:
    """Return the current time as an ISO-8601 string in America/Toronto timezone."""
    # LOGIC — TAC-7: timestamps must be ET, never UTC
    return datetime.now(_ET_TZ).isoformat()


def notify_success(desk_code: str, trade_date: str, summary: dict) -> None:
    """
    Publish a TRADE_POSITIONS_LOADED message to the success SNS topic.
    Satisfies: BAC-5, TAC-5.

    The summary dict is expected to contain at minimum:
        total_rows_received, rows_successfully_loaded, rows_rejected,
        report_s3_key, manifest_s3_key
    """
    # BOILERPLATE — topic ARN from environment; no hardcoded values
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    # LOGIC — build the canonical success message shape from the data contract
    payload = {
        "event": "TRADE_POSITIONS_LOADED",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": summary.get("total_rows_received"),
        "rows_successfully_loaded": summary.get("rows_successfully_loaded"),
        "rows_rejected": summary.get("rows_rejected"),
        "processing_timestamp_et": _get_et_timestamp(),
        "report_s3_key": summary.get("report_s3_key"),
        "manifest_s3_key": summary.get("manifest_s3_key"),
    }

    subject = f"TRADE_POSITIONS_LOADED | {desk_code} | {trade_date}"

    # BOILERPLATE — boto3 SNS client; IAM role provides credentials
    sns_client = boto3.client("sns")
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject=subject[:100],  # SNS subject max 100 chars
        )
        logger.info(
            "sns_notifier: success notification published | desk_code=%s trade_date=%s "
            "MessageId=%s",
            desk_code,
            trade_date,
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error(
            "sns_notifier: failed to publish success notification for %s %s: %s",
            desk_code,
            trade_date,
            exc,
        )
        raise


def notify_failure(filename: str, error_detail: str) -> None:
    """
    Publish a TRADE_POSITIONS_FAILED message to the failure SNS topic.
    Satisfies: BAC-5, TAC-5.
    """
    # BOILERPLATE — topic ARN from environment; no hardcoded values
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — build the canonical failure message shape from the data contract
    payload = {
        "event": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "error_detail": error_detail,
        "processing_timestamp_et": _get_et_timestamp(),
    }

    subject = f"TRADE_POSITIONS_FAILED | {filename}"

    # BOILERPLATE — boto3 SNS client; IAM role provides credentials
    sns_client = boto3.client("sns")
    try:
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject=subject[:100],  # SNS subject max 100 chars
        )
        logger.info(
            "sns_notifier: failure notification published | filename=%s MessageId=%s",
            filename,
            response.get("MessageId"),
        )
    except Exception as exc:
        logger.error(
            "sns_notifier: failed to publish failure notification for %s: %s",
            filename,
            exc,
        )
        raise