# BOILERPLATE
import json
import logging
from datetime import datetime
from typing import Optional

import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


# LOGIC
def notify_success(sns_client, topic_arn: str, report: dict) -> None:
    """Publish a success notification to the SNS success topic."""

    desk_code = report.get("desk_code", "UNKNOWN")
    trade_date = report.get("trade_date", "UNKNOWN")

    # LOGIC — build payload per SNS success message schema in data contracts
    payload = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": report.get("processing_timestamp", ""),
        "total_rows_received": report.get("total_rows_received", 0),
        "rows_inserted": report.get("rows_inserted", 0),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "status": report.get("status", "UNKNOWN"),
        "report_s3_key": report.get("report_s3_key", ""),
    }

    subject = f"TRADE_POSITIONS_LOADED: {desk_code} {trade_date}"
    message_body = json.dumps(payload)

    # BOILERPLATE — publish to SNS
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message_body,
    )

    message_id = response.get("MessageId", "unknown")
    logger.info(
        "Success notification published: MessageId=%s desk=%s date=%s",
        message_id,
        desk_code,
        trade_date,
    )


# LOGIC
def notify_failure(
    sns_client,
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    error_message: str,
    processing_ts: datetime,
    s3_key: str = "",
) -> None:
    """Publish a failure notification to the SNS failure topic."""

    # LOGIC — ISO 8601 timestamp with ET offset
    processing_timestamp_str = processing_ts.isoformat()

    # LOGIC — build payload per SNS failure message schema in data contracts
    payload = {
        "event_type": "TRADE_POSITIONS_FAILED",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp_str,
        "error_message": error_message,
        "s3_key": s3_key,
    }

    subject = f"TRADE_POSITIONS_FAILED: {desk_code} {trade_date}"
    message_body = json.dumps(payload)

    # BOILERPLATE — publish to SNS
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message_body,
    )

    message_id = response.get("MessageId", "unknown")
    logger.info(
        "Failure notification published: MessageId=%s desk=%s date=%s",
        message_id,
        desk_code,
        trade_date,
    )