# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE
_ET = pytz.timezone("America/Toronto")


def _sns_client():
    # BOILERPLATE — creates SNS client; relies on IAM role, no credentials in code
    return boto3.client("sns")


def send_success(report_dict: dict) -> None:
    # LOGIC — build the success SNS payload from the report dict
    payload = {
        "event_type": "POSITION_INGESTION_SUCCESS",
        "desk_code": report_dict["desk_code"],
        "trade_date": report_dict["trade_date"],
        "source_file": report_dict["source_file"],
        "processing_timestamp": report_dict["processing_timestamp"],
        "total_rows": report_dict["total_rows"],
        "rows_loaded": report_dict["rows_loaded"],
        "rows_rejected": report_dict["rows_rejected"],
        "min_notional": report_dict["min_notional"],
        "max_notional": report_dict["max_notional"],
    }

    desk_code = report_dict["desk_code"]
    trade_date = report_dict["trade_date"]
    subject = f"RFDH Position Ingestion SUCCESS — {desk_code} {trade_date}"

    topic_arn = os.environ["SNS_TOPIC_ARN"]

    client = _sns_client()
    try:
        client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject=subject,
        )
        logger.info(
            "Success notification published to SNS for desk=%s trade_date=%s",
            desk_code,
            trade_date,
        )
    except Exception as exc:
        logger.error(
            "Failed to publish success SNS notification: %s",
            exc,
            exc_info=True,
        )
        raise


def send_failure(error_details: dict) -> None:
    # LOGIC — build the failure SNS payload from the error_details dict
    processing_timestamp = error_details.get(
        "processing_timestamp",
        datetime.now(_ET).isoformat(),
    )

    payload = {
        "event_type": "POSITION_INGESTION_FAILURE",
        "desk_code": error_details.get("desk_code", "UNKNOWN"),
        "trade_date": error_details.get("trade_date", "UNKNOWN"),
        "source_file": error_details.get("source_file", "UNKNOWN"),
        "processing_timestamp": processing_timestamp,
        "error_type": error_details.get("error_type", "Exception"),
        "error_message": error_details.get("error_message", ""),
    }

    desk_code = payload["desk_code"]
    trade_date = payload["trade_date"]
    subject = f"RFDH Position Ingestion FAILURE — {desk_code} {trade_date}"

    topic_arn = os.environ["SNS_TOPIC_ARN"]

    client = _sns_client()
    try:
        client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(payload),
            Subject=subject,
        )
        logger.info(
            "Failure notification published to SNS for desk=%s trade_date=%s",
            desk_code,
            trade_date,
        )
    except Exception as exc:
        logger.error(
            "Failed to publish failure SNS notification: %s",
            exc,
            exc_info=True,
        )
        raise