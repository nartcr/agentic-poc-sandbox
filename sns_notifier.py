# BOILERPLATE
import json
import logging
from datetime import datetime

import time_utils

logger = logging.getLogger(__name__)


def notify_success(
    sns_client,
    topic_arn: str,
    filename: str,
    desk_code: str,
    trade_date_str: str,
    total_rows: int,
    rows_inserted: int,
    rows_rejected: int,
    report_key: str,
    processing_timestamp_et: datetime,
) -> None:
    # LOGIC — success message schema exactly as defined in TDD
    message = {
        "event": "TRADE_POSITIONS_LOADED",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date_str,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "report_s3_key": report_key,
        "processing_timestamp_et": time_utils.to_et_string(processing_timestamp_et),
    }

    message_str = json.dumps(message)

    logger.info(
        "Publishing success notification to SNS: topic=%s filename=%s rows_inserted=%d",
        topic_arn,
        filename,
        rows_inserted,
    )

    # LOGIC — does not swallow exceptions; failures propagate to pipeline_handler
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITIONS_LOADED",
    )

    logger.info("Success notification published: filename=%s", filename)


def notify_failure(
    sns_client,
    topic_arn: str,
    filename: str,
    error_message: str,
    processing_timestamp_et: datetime,
) -> None:
    # LOGIC — failure message schema exactly as defined in TDD
    message = {
        "event": "TRADE_POSITIONS_FAILED",
        "filename": filename,
        "error_message": error_message,
        "processing_timestamp_et": time_utils.to_et_string(processing_timestamp_et),
    }

    message_str = json.dumps(message)

    logger.info(
        "Publishing failure notification to SNS: topic=%s filename=%s error=%s",
        topic_arn,
        filename,
        error_message,
    )

    # LOGIC — does not swallow exceptions; failures propagate to pipeline_handler
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject="TRADE_POSITIONS_FAILED",
    )

    logger.info("Failure notification published: filename=%s", filename)