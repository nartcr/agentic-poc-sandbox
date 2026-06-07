# BOILERPLATE
import json
import logging

import boto3

from pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)


# LOGIC
def notify_success(config: PipelineConfig, report_dict: dict) -> None:
    """Publish a success message to the SNS success topic."""
    client = boto3.client("sns")

    # LOGIC — build the success message payload matching the DATA CONTRACT
    message_body = {
        "event_type": "TRADE_POSITIONS_LOADED",
        "desk_code": report_dict.get("desk_code"),
        "trade_date": report_dict.get("trade_date"),
        "source_file_key": report_dict.get("source_file_key"),
        "processed_at_et": report_dict.get("processed_at_et"),
        "total_rows_received": report_dict.get("total_rows_received"),
        "rows_loaded": report_dict.get("rows_loaded"),
        "rows_rejected": report_dict.get("rows_rejected"),
        "rows_skipped_duplicate": report_dict.get("rows_skipped_duplicate"),
        "report_s3_key": report_dict.get("report_s3_key"),
    }

    response = client.publish(
        TopicArn=config.sns_success_arn,
        Message=json.dumps(message_body),
        Subject="Trade Positions Loaded Successfully",
    )

    message_id = response.get("MessageId")
    logger.info(
        "SNS success notification published. MessageId=%s TopicArn=%s desk_code=%s trade_date=%s",
        message_id,
        config.sns_success_arn,
        message_body.get("desk_code"),
        message_body.get("trade_date"),
    )


# LOGIC
def notify_failure(config: PipelineConfig, error_details: dict) -> None:
    """Publish a failure message to the SNS failure topic."""
    client = boto3.client("sns")

    # LOGIC — build the failure message payload matching the DATA CONTRACT
    message_body = {
        "event_type": "TRADE_POSITIONS_FAILED",
        "source_file_key": error_details.get("source_file_key"),
        "desk_code": error_details.get("desk_code"),
        "trade_date": error_details.get("trade_date"),
        "processed_at_et": error_details.get("processed_at_et"),
        "error_message": error_details.get("error_message"),
        "error_s3_key": error_details.get("error_s3_key"),
    }

    response = client.publish(
        TopicArn=config.sns_failure_arn,
        Message=json.dumps(message_body),
        Subject="Trade Positions Processing Failed",
    )

    message_id = response.get("MessageId")
    logger.info(
        "SNS failure notification published. MessageId=%s TopicArn=%s source_file_key=%s",
        message_id,
        config.sns_failure_arn,
        message_body.get("source_file_key"),
    )