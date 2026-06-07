# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def notify_success(summary: dict) -> None:
    # LOGIC — build the success SNS payload from the summary dict
    desk_code = summary.get("desk_code", "")
    trade_date = summary.get("trade_date", "")
    filename = f"{desk_code}_{trade_date}_positions.csv"

    # LOGIC — report_s3_key is derived from desk_code and trade_date
    report_s3_key = f"reports/{desk_code}_{trade_date}_summary.json"

    payload = {
        "event_type": "TRADE_POSITION_LOAD_SUCCESS",
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows_received": summary.get("total_rows_received"),
        "rows_valid": summary.get("rows_valid"),
        "rows_inserted": summary.get("rows_inserted"),
        "rows_skipped_duplicate": summary.get("rows_skipped_duplicate"),
        "rows_rejected": summary.get("rows_rejected"),
        "processing_timestamp_et": summary.get("processing_timestamp_et"),
        "report_s3_key": report_s3_key,
        "notional_min": summary.get("notional_min"),
        "notional_max": summary.get("notional_max"),
    }

    # BOILERPLATE — read ARN from environment at call time
    success_arn = os.environ["SNS_SUCCESS_ARN"]

    # BOILERPLATE — SNS client instantiated locally to avoid module-level side effects
    sns_client = boto3.client("sns")

    message_body = json.dumps(payload)

    logger.info(
        "Publishing success notification to SNS topic: %s, filename: %s",
        success_arn,
        filename,
    )

    sns_client.publish(
        TopicArn=success_arn,
        Message=message_body,
        Subject="Trade Position Load Success",
    )

    logger.info(
        "Success notification published: filename=%s rows_inserted=%s",
        filename,
        payload.get("rows_inserted"),
    )


def notify_failure(
    filename: str,
    error_type: str,
    error_message: str,
    processing_timestamp_et: str,
) -> None:
    # LOGIC — build the failure SNS payload
    payload = {
        "event_type": "TRADE_POSITION_LOAD_FAILURE",
        "filename": filename,
        "error_type": error_type,
        "error_message": error_message,
        "processing_timestamp_et": processing_timestamp_et,
    }

    # BOILERPLATE — read ARN from environment at call time
    failure_arn = os.environ["SNS_FAILURE_ARN"]

    # BOILERPLATE — SNS client instantiated locally to avoid module-level side effects
    sns_client = boto3.client("sns")

    message_body = json.dumps(payload)

    logger.info(
        "Publishing failure notification to SNS topic: %s, filename: %s, error_type: %s",
        failure_arn,
        filename,
        error_type,
    )

    sns_client.publish(
        TopicArn=failure_arn,
        Message=message_body,
        Subject="Trade Position Load Failure",
    )

    logger.info(
        "Failure notification published: filename=%s error_type=%s",
        filename,
        error_type,
    )