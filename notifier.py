# BOILERPLATE
import json
import logging
import math
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)


# LOGIC
def notify_success(
    sns_client,
    topic_arn: str,
    report: dict,
    report_s3_key: str,
) -> None:
    # LOGIC — build SNS message body per SUCCESS topic data contract
    message_body = {
        "status": "SUCCESS",
        "desk_code": report["desk_code"],
        "trade_date": report["trade_date"],
        "processing_timestamp": report["processing_timestamp"],
        "total_rows": report["total_rows"],
        "rows_loaded": report["rows_loaded"],
        "rows_rejected": report["rows_rejected"],
        "notional_min": report["notional_min"],
        "notional_max": report["notional_max"],
        "report_s3_key": report_s3_key,
    }

    message_str = json.dumps(message_body, default=_json_default)

    # BOILERPLATE — publish to SNS
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=f"Position Load SUCCESS: {report['desk_code']} {report['trade_date']}",
    )

    logger.info(
        "Success notification published to %s for desk_code=%s trade_date=%s",
        topic_arn,
        report["desk_code"],
        report["trade_date"],
    )


# LOGIC
def notify_failure(
    sns_client,
    topic_arn: str,
    desk_code: str,
    trade_date: str,
    error_details: str,
    processing_ts: datetime,
    file_key: str,
) -> None:
    # LOGIC — ET ISO-8601 timestamp for failure notification
    et_tz = pytz.timezone("America/Toronto")
    if processing_ts.tzinfo is None:
        # LOGIC — if naive datetime supplied, localize to ET
        processing_ts = et_tz.localize(processing_ts)

    processing_timestamp = processing_ts.strftime("%Y-%m-%dT%H:%M:%S%z")

    # LOGIC — build SNS message body per FAILURE topic data contract
    message_body = {
        "status": "FAILURE",
        "desk_code": desk_code,
        "trade_date": trade_date,
        "processing_timestamp": processing_timestamp,
        "error_details": error_details,
        "file_key": file_key,
    }

    message_str = json.dumps(message_body)

    # BOILERPLATE — publish to SNS
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message_str,
        Subject=f"Position Load FAILURE: {desk_code} {trade_date}",
    )

    logger.info(
        "Failure notification published to %s for desk_code=%s trade_date=%s",
        topic_arn,
        desk_code,
        trade_date,
    )


# LOGIC — custom JSON serializer to handle float edge cases cleanly
def _json_default(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")