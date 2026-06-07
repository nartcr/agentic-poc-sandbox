# BOILERPLATE
import json
import logging
from datetime import date, datetime

import pytz

from pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def _serialize_report(report: dict) -> str:
    # LOGIC — serialize report dict to JSON string, handling date/datetime types
    def _default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(report, default=_default)


def notify_success(
    sns_client,
    config: PipelineConfig,
    report: dict,
) -> str:
    # LOGIC — publish success notification to the success SNS topic
    desk_code = report.get("desk_code", "UNKNOWN")
    trade_date = report.get("trade_date", "UNKNOWN")

    subject = f"TradePositionIngestion:{desk_code}:{trade_date}:SUCCESS"
    message_body = _serialize_report(report)

    logger.info(
        "Publishing success notification to SNS topic %s for desk_code=%s trade_date=%s",
        config.sns_success_arn,
        desk_code,
        trade_date,
    )

    response = sns_client.publish(
        TopicArn=config.sns_success_arn,
        Subject=subject,
        Message=message_body,
    )

    message_id = response["MessageId"]
    logger.info("SNS success notification published. MessageId=%s", message_id)
    return message_id


def notify_failure(
    sns_client,
    config: PipelineConfig,
    desk_code: str,
    trade_date: str,
    s3_key: str,
    error_message: str,
    partial_report: dict | None = None,
) -> str:
    # LOGIC — publish failure notification to the failure SNS topic
    subject = f"TradePositionIngestion:{desk_code}:{trade_date}:FAILURE"

    # LOGIC — build failure message body matching the SNS failure message contract
    now_et = datetime.now(_ET)
    processing_timestamp_str = now_et.isoformat()

    # LOGIC — if a partial report exists, pull the processing_timestamp from it for consistency
    if partial_report and "processing_timestamp" in partial_report:
        processing_timestamp_str = partial_report["processing_timestamp"]

    failure_payload = {
        "status": "FAILURE",
        "s3_key": s3_key,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp_str,
        "error_message": error_message,
        "partial_report": partial_report,
    }

    message_body = json.dumps(failure_payload)

    logger.info(
        "Publishing failure notification to SNS topic %s for desk_code=%s trade_date=%s",
        config.sns_failure_arn,
        desk_code,
        trade_date,
    )

    response = sns_client.publish(
        TopicArn=config.sns_failure_arn,
        Subject=subject,
        Message=message_body,
    )

    message_id = response["MessageId"]
    logger.info("SNS failure notification published. MessageId=%s", message_id)
    return message_id