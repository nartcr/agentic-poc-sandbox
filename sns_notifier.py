# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — field names used in SNS messages match the approved design contracts exactly
_SUCCESS_EVENT = "TRADE_POSITIONS_LOADED"
_FAILURE_EVENT = "TRADE_POSITIONS_FAILED"


def _get_sns_client():
    # BOILERPLATE — construct SNS client; credentials come from Lambda execution role
    return boto3.client("sns")


def notify_success(report: dict) -> None:
    # LOGIC — build success message from the report dict using only the fields
    # specified in the approved design SNS contract
    success_topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    message_payload = {
        "event": _SUCCESS_EVENT,
        "filename": report["filename"],
        "desk_code": report["desk_code"],
        "trade_date": report["trade_date"],
        "processing_timestamp_et": report["processing_timestamp_et"],
        "total_rows_received": report["total_rows_received"],
        "rows_successfully_loaded": report["rows_successfully_loaded"],
        "rows_rejected": report["rows_rejected"],
        "rows_skipped_duplicate": report["rows_skipped_duplicate"],
    }

    # LOGIC — SNS publish() requires a string Message, not a dict
    message_str = json.dumps(message_payload)

    subject = (
        f"[TRADE POSITIONS LOADED] {report.get('desk_code', 'UNKNOWN')} "
        f"/ {report.get('trade_date', 'UNKNOWN')}"
    )

    sns_client = _get_sns_client()
    sns_client.publish(
        TopicArn=success_topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.info(
        "Success SNS notification published: filename=%s desk_code=%s trade_date=%s "
        "rows_loaded=%d rows_rejected=%d",
        report.get("filename"),
        report.get("desk_code"),
        report.get("trade_date"),
        report.get("rows_successfully_loaded", 0),
        report.get("rows_rejected", 0),
    )


def notify_failure(filename: str, error: str, processing_ts_et: datetime) -> None:
    # LOGIC — build failure message per the approved design SNS contract
    failure_topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — TAC-7: serialize timestamp as ISO-8601 with ET offset
    if processing_ts_et.tzinfo is None:
        logger.warning(
            "notify_failure received a naive datetime for processing_ts_et; "
            "the published timestamp will lack a UTC offset."
        )
    ts_iso = processing_ts_et.isoformat()

    message_payload = {
        "event": _FAILURE_EVENT,
        "filename": filename,
        "processing_timestamp_et": ts_iso,
        "error": error,
    }

    # LOGIC — SNS publish() requires a string Message, not a dict
    message_str = json.dumps(message_payload)

    subject = f"[TRADE POSITIONS FAILED] {filename}"

    sns_client = _get_sns_client()
    sns_client.publish(
        TopicArn=failure_topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.error(
        "Failure SNS notification published: filename=%s error=%s",
        filename,
        error,
    )