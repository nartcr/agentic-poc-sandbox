# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# BOILERPLATE — module-level SNS client; reused across Lambda warm invocations
_sns_client = None


def _get_sns_client():
    """Return a cached boto3 SNS client (IAM role credentials, no hardcoded secrets)."""
    # BOILERPLATE
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns")
    return _sns_client


class _DatetimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that serialises datetime objects to ISO-8601 strings."""

    # LOGIC
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


# LOGIC
def notify_success(summary: dict) -> None:
    """Publish a TRADE_POSITIONS_LOADED notification to the success SNS topic.

    Expected keys in *summary*:
        desk_code, trade_date, rows_loaded, rows_rejected,
        rows_skipped_duplicate, report_s3_key, processing_timestamp_et

    Args:
        summary: Dict containing all success-message fields built by pipeline_handler.
    """
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]

    # LOGIC — build the canonical success message schema from the design spec
    message_body = {
        "event": "TRADE_POSITIONS_LOADED",
        "desk_code": summary["desk_code"],
        "trade_date": summary["trade_date"],
        "rows_loaded": summary["rows_loaded"],
        "rows_rejected": summary["rows_rejected"],
        "rows_skipped_duplicate": summary["rows_skipped_duplicate"],
        "report_s3_key": summary["report_s3_key"],
        "processing_timestamp_et": summary["processing_timestamp_et"],
    }

    # LOGIC — serialise timestamp fields (datetime → ISO-8601 string)
    message_str = json.dumps(message_body, cls=_DatetimeEncoder)

    desk_code = summary.get("desk_code", "UNKNOWN")
    trade_date = summary.get("trade_date", "UNKNOWN")
    subject = f"Trade Positions Loaded: {desk_code} {trade_date}"

    sns = _get_sns_client()
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.info(
        "SNS success notification published: topic=%s MessageId=%s desk_code=%s trade_date=%s",
        topic_arn,
        response.get("MessageId"),
        desk_code,
        trade_date,
    )


# LOGIC
def notify_failure(error_detail: dict) -> None:
    """Publish a TRADE_POSITIONS_FAILED notification to the failure SNS topic.

    Expected keys in *error_detail*:
        filename, error, processing_timestamp_et

    Args:
        error_detail: Dict containing failure context built by pipeline_handler.
    """
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — build the canonical failure message schema from the design spec
    message_body = {
        "event": "TRADE_POSITIONS_FAILED",
        "filename": error_detail.get("filename", "UNKNOWN"),
        "error": error_detail.get("error", "Unknown error"),
        "processing_timestamp_et": error_detail.get("processing_timestamp_et"),
    }

    # LOGIC — serialise timestamp fields (datetime → ISO-8601 string)
    message_str = json.dumps(message_body, cls=_DatetimeEncoder)

    filename = error_detail.get("filename", "UNKNOWN")
    subject = f"Trade Position Ingestion FAILED: {filename}"

    sns = _get_sns_client()
    response = sns.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=subject,
    )

    logger.info(
        "SNS failure notification published: topic=%s MessageId=%s filename=%s",
        topic_arn,
        response.get("MessageId"),
        filename,
    )