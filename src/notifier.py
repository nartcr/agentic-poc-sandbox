# BOILERPLATE
import json
import logging
import datetime
from typing import Optional

import pytz

logger = logging.getLogger(__name__)

# LOGIC — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def _format_et(dt: datetime.datetime) -> str:
    """Return ISO 8601 string of dt in ET, e.g. '2026-06-01T20:15:33.123456-04:00'."""
    # LOGIC — convert to ET if not already, then isoformat
    dt_et = dt.astimezone(_ET)
    return dt_et.isoformat()


def publish_success(
    sns_client,
    topic_arn: str,
    report: dict,
) -> str:
    """
    Publish a TRADE_INGESTION_SUCCESS event to SNS.
    Reads all fields from the report dict.
    Returns the SNS MessageId.
    """
    # LOGIC — build the canonical success payload as defined in the data contract
    payload = {
        "event": "TRADE_INGESTION_SUCCESS",
        "source_file": report["source_file"],
        "total_rows_received": report["total_rows_received"],
        "rows_loaded": report["rows_loaded"],
        "rows_rejected": report["rows_rejected"],
        "rows_skipped_duplicate": report["rows_skipped_duplicate"],
        "load_timestamp": report["load_timestamp"],
        "report_key": report.get("report_key", ""),
    }

    message_body: str = json.dumps(payload)

    # BOILERPLATE — SNS publish
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject="TRADE_INGESTION_SUCCESS",
    )

    message_id: str = response["MessageId"]
    logger.info(
        "SNS success notification published: MessageId=%s source=%s",
        message_id,
        report["source_file"],
    )
    return message_id


def publish_failure(
    sns_client,
    topic_arn: str,
    source_key: str,
    error_message: str,
    failed_at: datetime.datetime,
) -> str:
    """
    Publish a TRADE_INGESTION_FAILURE event to SNS.
    Returns the SNS MessageId.
    """
    # LOGIC — build the canonical failure payload as defined in the data contract
    payload = {
        "event": "TRADE_INGESTION_FAILURE",
        "source_file": source_key,
        "error_message": error_message,
        "failed_at": _format_et(failed_at),
    }

    message_body: str = json.dumps(payload)

    # BOILERPLATE — SNS publish
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_body,
        Subject="TRADE_INGESTION_FAILURE",
    )

    message_id: str = response["MessageId"]
    logger.info(
        "SNS failure notification published: MessageId=%s source=%s error=%s",
        message_id,
        source_key,
        error_message,
    )
    return message_id