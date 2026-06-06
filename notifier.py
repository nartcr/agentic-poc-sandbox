# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import pytz

from config import Config

logger = logging.getLogger(__name__)


def publish_success(topic_arn: str, report: dict) -> str:
    # LOGIC — build success message per SNS data contract
    message_body = {
        "event_type": "INGESTION_SUCCESS",
        "source_file": report.get("source_file"),
        "desk_code": _extract_desk_code_from_report(report),
        "trade_date": _extract_trade_date_from_report(report),
        "total_rows_received": report.get("total_rows_received"),
        "rows_loaded": report.get("rows_loaded"),
        "rows_rejected": report.get("rows_rejected"),
        "load_timestamp": report.get("load_timestamp"),
        "report_s3_key": report.get("report_s3_key"),
        "error_file_s3_key": report.get("error_file_s3_key"),
        "min_notional_amount": report.get("min_notional_amount"),
        "max_notional_amount": report.get("max_notional_amount"),
        "record_counts_by_desk_code": report.get("record_counts_by_desk_code", {}),
        "null_rates": report.get("null_rates", {}),
    }

    message_str = json.dumps(message_body, default=str)

    # BOILERPLATE — publish to SNS
    sns_client = boto3.client("sns", region_name=Config.AWS_REGION)
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="Trade Position Ingestion: SUCCESS",
    )

    message_id = response["MessageId"]
    logger.info(
        "Success SNS notification published: MessageId=%s TopicArn=%s",
        message_id,
        topic_arn,
    )
    return message_id


def publish_failure(
    topic_arn: str,
    source_file: str,
    error_type: str,
    error_detail: str,
) -> str:
    # LOGIC — build failure message per SNS data contract
    et_tz = pytz.timezone("America/Toronto")
    timestamp_et = datetime.now(et_tz).isoformat()

    message_body = {
        "event_type": "INGESTION_FAILURE",
        "source_file": source_file,
        "error_type": error_type,
        "error_detail": error_detail,
        "timestamp": timestamp_et,
    }

    message_str = json.dumps(message_body, default=str)

    # BOILERPLATE — publish to SNS
    sns_client = boto3.client("sns", region_name=Config.AWS_REGION)
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="Trade Position Ingestion: FAILURE",
    )

    message_id = response["MessageId"]
    logger.info(
        "Failure SNS notification published: MessageId=%s TopicArn=%s",
        message_id,
        topic_arn,
    )
    return message_id


def _extract_desk_code_from_report(report: dict) -> str:
    # LOGIC — derive desk_code from record_counts_by_desk_code or source_file
    # Prefer parsing from source_file as the single authoritative source
    source_file = report.get("source_file", "")
    return _parse_desk_code_from_key(source_file)


def _extract_trade_date_from_report(report: dict) -> str:
    # LOGIC — derive trade_date from source_file key
    source_file = report.get("source_file", "")
    return _parse_trade_date_from_key(source_file)


def _parse_desk_code_from_key(source_file_key: str) -> str:
    # LOGIC — extract desk_code from filename pattern {desk_code}_{trade_date}_positions.csv
    import os
    basename = os.path.basename(source_file_key)
    stem = basename.replace("_positions.csv", "")
    # trade_date is always the last 10 chars (YYYY-MM-DD)
    if len(stem) > 11:
        return stem[: -(10 + 1)]
    return stem


def _parse_trade_date_from_key(source_file_key: str) -> str:
    # LOGIC — extract trade_date from filename pattern {desk_code}_{trade_date}_positions.csv
    import os
    basename = os.path.basename(source_file_key)
    stem = basename.replace("_positions.csv", "")
    # trade_date is always the last 10 chars (YYYY-MM-DD)
    if len(stem) >= 10:
        return stem[-10:]
    return stem