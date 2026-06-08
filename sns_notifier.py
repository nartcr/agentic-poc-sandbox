# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import boto3
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC — derive manifest key from desk_code and trade_date
def _build_manifest_key(desk_code: str, trade_date: str) -> str:
    """Returns the predictable manifest S3 key for a given desk and date."""
    return f"manifests/{desk_code}_{trade_date}_manifest.json"


# LOGIC
def notify_success(
    desk_code: str,
    trade_date: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    report_s3_key: str,
    processing_timestamp_et: str,
) -> None:
    """
    Publishes a success notification to the SNS success topic.
    Message body is a JSON string conforming to the data contract.
    """
    topic_arn = os.environ["SNS_SUCCESS_TOPIC_ARN"]
    manifest_s3_key = _build_manifest_key(desk_code, trade_date)

    # LOGIC — assemble message body per data contract
    message_body = {
        "event": "TRADE_POSITION_LOAD_SUCCESS",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "report_s3_key": report_s3_key,
        "manifest_s3_key": manifest_s3_key,
        "processing_timestamp_et": processing_timestamp_et,
    }

    subject = f"Trade Position Load Success: {desk_code} {trade_date}"

    logger.info(
        "Publishing success notification to SNS: desk_code=%s trade_date=%s "
        "rows_inserted=%d rows_rejected=%d topic_arn=%s",
        desk_code,
        trade_date,
        rows_inserted,
        rows_rejected,
        topic_arn,
    )

    # BOILERPLATE — publish to SNS
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body),
        Subject=subject,
    )

    logger.info(
        "Success notification published for desk_code=%s trade_date=%s",
        desk_code,
        trade_date,
    )


# LOGIC
def notify_failure(
    filename: str,
    error_message: str,
    processing_timestamp_et: str,
) -> None:
    """
    Publishes a failure notification to the SNS failure topic.
    Message body is a JSON string conforming to the data contract.
    No stack traces or credential values are included in the message.
    """
    topic_arn = os.environ["SNS_FAILURE_TOPIC_ARN"]

    # LOGIC — assemble message body per data contract
    message_body = {
        "event": "TRADE_POSITION_LOAD_FAILED",
        "filename": filename,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    subject = f"Trade Position Load FAILED: {filename}"

    logger.info(
        "Publishing failure notification to SNS: filename=%s topic_arn=%s",
        filename,
        topic_arn,
    )

    # BOILERPLATE — publish to SNS
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message_body),
        Subject=subject,
    )

    logger.info("Failure notification published for filename=%s", filename)