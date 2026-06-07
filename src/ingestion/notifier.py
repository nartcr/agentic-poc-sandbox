# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)

# LOGIC
def notify_success(summary: dict, sns_client, topic_arn: str) -> None:
    """Publish a success SNS notification containing the ingestion summary."""
    source_file = summary.get("source_file", "")

    message_body = {
        "event_type": "TRADE_INGESTION_SUCCESS",
        "source_file": source_file,
        "processed_at": summary.get("processed_at", ""),
        "total_rows_received": summary.get("total_rows_received", 0),
        "rows_loaded": summary.get("rows_loaded", 0),
        "rows_rejected": summary.get("rows_rejected", 0),
        "rows_skipped_duplicate": summary.get("rows_skipped_duplicate", 0),
        "desk_counts": summary.get("desk_counts", {}),
        "min_notional": summary.get("min_notional", None),
        "max_notional": summary.get("max_notional", None),
        "report_s3_key": summary.get("report_s3_key", ""),
    }

    subject = f"Trade Position Ingestion Succeeded: {source_file}"
    # SNS subject max length is 100 characters
    subject = subject[:100]

    logger.info("Publishing success SNS notification for source_file=%s", source_file)

    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body),
        Subject=subject,
    )

    logger.info("Success SNS notification published for source_file=%s", source_file)


# LOGIC
def notify_failure(
    source_key: str,
    error_message: str,
    sns_client,
    topic_arn: str,
) -> None:
    """Publish a failure SNS notification with error details."""
    et_tz = pytz.timezone("America/Toronto")
    failed_at = datetime.now(et_tz).isoformat()

    message_body = {
        "event_type": "TRADE_INGESTION_FAILURE",
        "source_file": source_key,
        "failed_at": failed_at,
        "error_message": error_message,
    }

    subject = f"Trade Position Ingestion Failed: {source_key}"
    # SNS subject max length is 100 characters
    subject = subject[:100]

    logger.info("Publishing failure SNS notification for source_key=%s", source_key)

    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body),
        Subject=subject,
    )

    logger.info("Failure SNS notification published for source_key=%s", source_key)