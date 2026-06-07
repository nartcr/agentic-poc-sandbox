# BOILERPLATE
import json
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone constant used for timestamp generation
_ET = pytz.timezone("America/Toronto")


def notify_success(sns_client, topic_arn: str, summary: dict) -> None:
    """
    Publishes a TRADE_POSITIONS_LOADED message to the success SNS topic.

    Expected keys in `summary`:
        filename, desk_code, trade_date, rows_loaded, rows_rejected,
        report_s3_key, processing_timestamp_et
    """
    # LOGIC — build success message payload matching approved schema
    message = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "filename": summary.get("filename"),
        "desk_code": summary.get("desk_code"),
        "trade_date": summary.get("trade_date"),
        "rows_loaded": summary.get("rows_loaded"),
        "rows_rejected": summary.get("rows_rejected"),
        "report_s3_key": summary.get("report_s3_key"),
        "processing_timestamp_et": summary.get("processing_timestamp_et"),
    }

    message_str = json.dumps(message, default=str)

    subject = f"Trade Positions Loaded: {summary.get('desk_code')} {summary.get('trade_date')}"

    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message_str,
            Subject=subject[:100],  # SNS subject max length is 100 chars
        )
        logger.info(
            "Success notification published to %s for filename=%s",
            topic_arn,
            summary.get("filename"),
        )
    except Exception as exc:
        logger.error(
            "Failed to publish success notification to %s: %s",
            topic_arn,
            exc,
            exc_info=True,
        )
        raise


def notify_failure(
    sns_client,
    topic_arn: str,
    filename: str,
    error: str,
    desk_code,
    trade_date_str,
) -> None:
    """
    Publishes a TRADE_POSITIONS_FAILED message to the failure SNS topic.

    Parameters
    ----------
    sns_client      : boto3 SNS client
    topic_arn       : SNS_FAILURE_ARN
    filename        : original S3 object key or filename
    error           : error message string
    desk_code       : parsed desk code, or None on early failure
    trade_date_str  : trade date string (YYYY-MM-DD), or None on early failure
    """
    # LOGIC — capture current ET timestamp at publish time
    processing_ts_et = datetime.now(_ET).isoformat()

    # LOGIC — build failure message payload matching approved schema
    message = {
        "event_type": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "error": error,
        "processing_timestamp_et": processing_ts_et,
    }

    message_str = json.dumps(message, default=str)

    subject = f"Trade Positions FAILED: {filename}"

    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message_str,
            Subject=subject[:100],  # SNS subject max length is 100 chars
        )
        logger.info(
            "Failure notification published to %s for filename=%s error=%s",
            topic_arn,
            filename,
            error,
        )
    except Exception as exc:
        logger.error(
            "Failed to publish failure notification to %s: %s",
            topic_arn,
            exc,
            exc_info=True,
        )
        raise