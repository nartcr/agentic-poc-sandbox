# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import pytz

from src.config import TIMEZONE

# BOILERPLATE
logger = logging.getLogger(__name__)


def _now_et_iso() -> str:
    # LOGIC — always produce an ET-aware ISO-8601 timestamp at call time
    return datetime.now(tz=TIMEZONE).isoformat()


def _derive_report_key(source_file: str) -> str:
    # LOGIC — reconstruct reports/{desk_code}_{trade_date}_report.json
    # from the source file path incoming/{desk_code}_{trade_date}_positions.csv
    filename = source_file.split("/")[-1]                 # e.g. EQDSK_2026-06-15_positions.csv
    stem = filename.replace("_positions.csv", "")         # e.g. EQDSK_2026-06-15
    return f"reports/{stem}_report.json"


def notify_success(report: dict, sns_success_arn: str) -> None:
    # LOGIC — build the mandated success message payload
    message_body = {
        "event": "TRADE_POSITIONS_LOADED",
        "source_file": report.get("source_file", ""),
        "processing_timestamp": report.get("processing_timestamp", _now_et_iso()),
        "total_rows_received": report.get("total_rows_received", 0),
        "rows_loaded": report.get("rows_loaded", 0),
        "rows_rejected": report.get("rows_rejected", 0),
        "report_s3_key": _derive_report_key(report.get("source_file", "")),
    }

    try:
        # BOILERPLATE — fresh client per call; no module-level client
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=sns_success_arn,
            Message=json.dumps(message_body),
            Subject="TRADE_POSITIONS_LOADED",
        )
        logger.info(
            "Success notification published: source_file=%s topic=%s",
            message_body["source_file"],
            sns_success_arn,
        )
    except Exception as exc:  # LOGIC — never raise; log and return
        logger.error(
            "Failed to publish success notification to %s: %s",
            sns_success_arn,
            exc,
        )


def notify_failure(
    source_key: str,
    error_message: str,
    sns_failure_arn: str,
) -> None:
    # LOGIC — build the mandated failure message payload
    message_body = {
        "event": "TRADE_POSITIONS_FAILED",
        "source_file": source_key,
        "processing_timestamp": _now_et_iso(),
        "error_message": error_message,
    }

    try:
        # BOILERPLATE — fresh client per call
        sns = boto3.client("sns")
        sns.publish(
            TopicArn=sns_failure_arn,
            Message=json.dumps(message_body),
            Subject="TRADE_POSITIONS_FAILED",
        )
        logger.info(
            "Failure notification published: source_file=%s topic=%s",
            source_key,
            sns_failure_arn,
        )
    except Exception as exc:  # LOGIC — never raise; log and return
        logger.error(
            "Failed to publish failure notification to %s: %s",
            sns_failure_arn,
            exc,
        )