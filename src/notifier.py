# BOILERPLATE
import json
import logging

logger = logging.getLogger(__name__)


def notify_success(sns_client, topic_arn: str, report: dict) -> None:
    # LOGIC — construct SNS success message body matching the data contract schema
    message_body = {
        "event": "TRADE_POSITION_INGESTION_SUCCESS",
        "source_file": report.get("source_file", ""),
        "desk_code": report.get("desk_code", ""),
        "trade_date": report.get("trade_date", ""),
        "processing_timestamp_et": report.get("processing_timestamp_et", ""),
        "total_rows_received": report.get("total_rows_received", 0),
        "rows_loaded": report.get("rows_loaded", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "rows_skipped_duplicate": report.get("rows_skipped_duplicate", 0),
        "notional_min": report.get("notional_min"),
        "notional_max": report.get("notional_max"),
    }

    # LOGIC — build subject line per data contract
    subject = (
        f"Trade Position Ingestion SUCCESS: "
        f"{report.get('desk_code', '')} {report.get('trade_date', '')}"
    )

    # LOGIC — SNS Message field must be a string; serialize the body dict
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body, default=str),
        Subject=subject,
    )

    logger.info(
        "Success notification published to %s for desk_code=%s trade_date=%s",
        topic_arn,
        report.get("desk_code", ""),
        report.get("trade_date", ""),
    )


def notify_failure(
    sns_client,
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    error_message: str,
    source_key: str,
) -> None:
    # LOGIC — construct SNS failure message body matching the data contract schema
    message_body = {
        "event": "TRADE_POSITION_INGESTION_FAILURE",
        "source_file": source_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "error_message": error_message,
    }

    # LOGIC — build subject line per data contract
    subject = f"Trade Position Ingestion FAILURE: {desk_code} {trade_date}"

    # LOGIC — SNS Message field must be a string; serialize the body dict
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body, default=str),
        Subject=subject,
    )

    logger.info(
        "Failure notification published to %s for desk_code=%s trade_date=%s error=%s",
        topic_arn,
        desk_code,
        trade_date,
        error_message,
    )