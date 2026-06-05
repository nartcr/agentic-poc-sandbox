# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def publish_success(report: dict, topic_arn: str) -> None:
    """
    # LOGIC
    Publishes a TRADE_LOAD_SUCCESS SNS message built from the report dict.
    Raises RuntimeError on SNS publish failure.
    """
    payload = {
        "event": "TRADE_LOAD_SUCCESS",
        "source_file": report["source_file"],
        "trade_date": report["trade_date"],
        "desk_code": report["desk_code"],
        "load_timestamp": report["load_timestamp"],
        "total_rows_received": report["total_rows_received"],
        "rows_loaded": report["rows_loaded"],
        "rows_rejected": report["rows_rejected"],
        "rows_skipped_duplicate": report["rows_skipped_duplicate"],
    }

    _publish(
        topic_arn=topic_arn,
        subject="TRADE_LOAD_SUCCESS",
        payload=payload,
    )
    logger.info(
        "Success notification published for '%s' to topic '%s'",
        report["source_file"],
        topic_arn,
    )


def publish_failure(source_file: str, error_message: str, topic_arn: str) -> None:
    """
    # LOGIC
    Publishes a TRADE_LOAD_FAILURE SNS message.
    failure_timestamp is always ET.
    Raises RuntimeError on SNS publish failure.
    """
    failure_timestamp = datetime.now(_ET).isoformat()

    payload = {
        "event": "TRADE_LOAD_FAILURE",
        "source_file": source_file,
        "error_message": error_message,
        "failure_timestamp": failure_timestamp,
    }

    _publish(
        topic_arn=topic_arn,
        subject="TRADE_LOAD_FAILURE",
        payload=payload,
    )
    logger.info(
        "Failure notification published for '%s' to topic '%s'",
        source_file,
        topic_arn,
    )


def _publish(topic_arn: str, subject: str, payload: dict) -> None:
    """
    # LOGIC
    Internal helper: serialises payload to JSON and calls SNS publish.
    Raises RuntimeError wrapping the original exception on any failure.
    """
    sns_client = boto3.client("sns")
    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject=subject,
        )
    except Exception as exc:
        logger.error(
            "SNS publish failed for subject='%s' topic='%s': %s",
            subject,
            topic_arn,
            exc,
        )
        raise RuntimeError(
            f"SNS publish failed for subject='{subject}' topic='{topic_arn}': {exc}"
        ) from exc