# BOILERPLATE
import json
import logging
import os

import boto3
import pytz
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def notify_success(summary: dict) -> None:
    # LOGIC — publish success notification to SNS with structured payload matching Data Contracts
    desk_code = summary.get("desk_code", "")
    trade_date = summary.get("trade_date", "")

    payload = {
        "event_type": "TRADE_POSITION_INGESTION_SUCCESS",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": summary.get("total_rows_received"),
        "rows_inserted": summary.get("rows_inserted"),
        "rows_skipped_duplicate": summary.get("rows_skipped_duplicate"),
        "rows_rejected": summary.get("rows_rejected"),
        "processing_timestamp_et": summary.get("processing_timestamp_et"),
        "report_s3_key": summary.get("report_s3_key"),
        "error_file_s3_key": summary.get("error_file_s3_key"),
    }

    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]
    subject = f"Trade Position Ingestion SUCCESS — {desk_code} {trade_date}"

    # BOILERPLATE — boto3 SNS client, no credentials in code
    sns_client = boto3.client("sns")

    # LOGIC — publish JSON-serialized payload to success topic
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject=subject,
    )

    logger.info(
        "SNS success notification published for desk_code=%s trade_date=%s topic_arn=%s",
        desk_code,
        trade_date,
        topic_arn,
    )


def notify_failure(error_details: dict) -> None:
    # LOGIC — publish failure notification to SNS with structured payload matching Data Contracts
    desk_code = error_details.get("desk_code")
    trade_date = error_details.get("trade_date")

    # LOGIC — ensure processing_timestamp_et is present; generate one in ET if caller omitted it
    processing_timestamp_et = error_details.get("processing_timestamp_et")
    if not processing_timestamp_et:
        et_tz = pytz.timezone("America/Toronto")
        processing_timestamp_et = datetime.now(et_tz).isoformat()

    payload = {
        "event_type": "TRADE_POSITION_INGESTION_FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "s3_input_key": error_details.get("s3_input_key"),
        "error_type": error_details.get("error_type"),
        "error_message": error_details.get("error_message"),
        "processing_timestamp_et": processing_timestamp_et,
    }

    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — subject uses desk_code/trade_date which may be None on early failures
    desk_code_str = desk_code if desk_code is not None else "UNKNOWN"
    trade_date_str = trade_date if trade_date is not None else "UNKNOWN"
    subject = f"Trade Position Ingestion FAILURE — {desk_code_str} {trade_date_str}"

    # BOILERPLATE — boto3 SNS client, no credentials in code
    sns_client = boto3.client("sns")

    # LOGIC — publish JSON-serialized payload to failure topic
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(payload),
        Subject=subject,
    )

    logger.info(
        "SNS failure notification published for desk_code=%s trade_date=%s topic_arn=%s",
        desk_code_str,
        trade_date_str,
        topic_arn,
    )