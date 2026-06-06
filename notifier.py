import json
import logging
import os
from datetime import datetime

import boto3
import pytz

from exceptions import NotificationError

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — ET timestamp helper used for failure notifications
def _et_now_iso() -> str:
    et = pytz.timezone("America/Toronto")
    return datetime.now(et).isoformat()


# LOGIC — safe JSON serializer that converts non-serializable types to None or str
def _safe_default(obj):
    if isinstance(obj, float):
        import math
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def notify_success(report_dict: dict) -> None:
    # LOGIC — build the SNS success message payload from report_dict fields
    topic_arn = os.environ["SNS_TOPIC_ARN"]

    desk_code = report_dict.get("desk_code", "")
    trade_date = report_dict.get("trade_date", "")

    message_body = {
        "event": "position_ingestion_success",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "source_file": report_dict.get("source_file", ""),
        "total_rows": report_dict.get("total_rows"),
        "rows_loaded": report_dict.get("rows_loaded"),
        "rows_rejected": report_dict.get("rows_rejected"),
        "processing_timestamp": report_dict.get("processing_timestamp"),
        "min_notional": report_dict.get("min_notional"),
        "max_notional": report_dict.get("max_notional"),
        "null_rates": report_dict.get("null_rates", {}),
    }

    subject = f"RFDH Position Ingestion SUCCESS: {desk_code} {trade_date}"

    # LOGIC — truncate subject to SNS 100-char limit
    subject = subject[:100]

    try:
        client = boto3.client("sns")
        client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=json.dumps(message_body, default=_safe_default),
            MessageStructure="string",
        )
        logger.info(
            "SNS success notification published for desk_code=%s trade_date=%s",
            desk_code,
            trade_date,
        )
    except Exception as exc:
        logger.error(
            "Failed to publish SNS success notification: %s", str(exc), exc_info=True
        )
        raise NotificationError(f"SNS publish failed: {exc}") from exc


def notify_failure(source_file: str, error: str) -> None:
    # LOGIC — build the SNS failure message payload
    topic_arn = os.environ["SNS_TOPIC_ARN"]

    message_body = {
        "event": "position_ingestion_failure",
        "source_file": source_file,
        "error": error,
        "processing_timestamp": _et_now_iso(),
    }

    subject = f"RFDH Position Ingestion FAILURE: {source_file}"
    subject = subject[:100]

    try:
        client = boto3.client("sns")
        client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=json.dumps(message_body, default=_safe_default),
            MessageStructure="string",
        )
        logger.info(
            "SNS failure notification published for source_file=%s", source_file
        )
    except Exception as exc:
        logger.error(
            "Failed to publish SNS failure notification: %s", str(exc), exc_info=True
        )
        raise NotificationError(f"SNS publish failed: {exc}") from exc