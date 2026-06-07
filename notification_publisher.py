# BOILERPLATE
import json
import logging
import boto3
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


def _sns_client():
    # BOILERPLATE
    return boto3.client("sns")


def publish_success(topic_arn: str, report: dict) -> None:
    # LOGIC — build success payload matching DATA CONTRACTS SNS schema exactly
    payload = {
        "event_type": "POSITIONS_LOADED",
        "file_name": report.get("file_name", ""),
        "desk_code": report.get("desk_code", ""),
        "trade_date": report.get("trade_date", ""),
        "total_rows_received": report.get("total_rows_received"),
        "rows_successfully_loaded": report.get("rows_successfully_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate"),
        "processing_timestamp": report.get("processing_timestamp"),
        "report_s3_key": report.get("report_s3_key"),
        "desk_code_counts": report.get("desk_code_counts"),
        "min_notional": report.get("min_notional"),
        "max_notional": report.get("max_notional"),
        "null_rates": report.get("null_rates"),
    }

    # LOGIC — publish; do not raise on failure so audit trail is never corrupted
    try:
        client = _sns_client()
        response = client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject="POSITIONS_LOADED: " + payload["desk_code"] + " " + payload["trade_date"],
        )
        logger.info(
            "SNS success notification published. MessageId=%s TopicArn=%s",
            response["MessageId"],
            topic_arn,
        )
    except Exception:
        logger.error(
            "Failed to publish SNS success notification to %s. Continuing without notification.",
            topic_arn,
            exc_info=True,
        )


def publish_failure(topic_arn: str, error_details: dict) -> None:
    # LOGIC — build failure payload matching DATA CONTRACTS SNS schema
    payload = {
        "event_type": "POSITIONS_FAILED",
        "file_name": error_details.get("file_name", ""),
        "desk_code": error_details.get("desk_code", ""),
        "trade_date": error_details.get("trade_date", ""),
        "error_message": error_details.get("error_message", ""),
        "timestamp": datetime.now(_ET).isoformat(),
    }

    # LOGIC — publish; do not raise on failure so audit trail is never corrupted
    try:
        client = _sns_client()
        response = client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject="POSITIONS_FAILED: " + payload["desk_code"] + " " + payload["trade_date"],
        )
        logger.info(
            "SNS failure notification published. MessageId=%s TopicArn=%s",
            response["MessageId"],
            topic_arn,
        )
    except Exception:
        logger.error(
            "Failed to publish SNS failure notification to %s. Continuing without notification.",
            topic_arn,
            exc_info=True,
        )