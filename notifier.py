# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import botocore.exceptions
import pytz

from exceptions import NotificationError
import config

logger = logging.getLogger(__name__)


def notify_success(report: dict) -> None:
    """
    Publish a success notification to SNS_TOPIC_ARN_SUCCESS.

    Raises NotificationError on boto3 failure.
    """
    # BOILERPLATE
    try:
        sns_client = boto3.client("sns", region_name=config.AWS_REGION)
        sns_client.publish(
            TopicArn=config.SNS_TOPIC_ARN_SUCCESS,
            Subject="Trade Position Load Complete",
            Message=json.dumps(report, default=str),
        )
        logger.info("Success notification published to %s", config.SNS_TOPIC_ARN_SUCCESS)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise NotificationError(
            f"SNS ClientError [{error_code}] publishing success notification."
        ) from exc
    except Exception as exc:
        raise NotificationError(
            f"Unexpected error publishing success notification: {type(exc).__name__}"
        ) from exc


def notify_failure(source_file: str, error_type: str, error_message: str) -> None:
    """
    Publish a failure notification to SNS_TOPIC_ARN_FAILURE.

    Raises NotificationError on boto3 failure.
    """
    # LOGIC — build failure payload with ET timestamp
    failure_payload = {
        "source_file": source_file,
        "error_type": error_type,
        "error_message": error_message,
        "failed_at": datetime.now(pytz.timezone("America/Toronto")).isoformat(),
    }

    # BOILERPLATE
    try:
        sns_client = boto3.client("sns", region_name=config.AWS_REGION)
        sns_client.publish(
            TopicArn=config.SNS_TOPIC_ARN_FAILURE,
            Subject="Trade Position Load FAILED",
            Message=json.dumps(failure_payload, default=str),
        )
        logger.info("Failure notification published to %s", config.SNS_TOPIC_ARN_FAILURE)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise NotificationError(
            f"SNS ClientError [{error_code}] publishing failure notification."
        ) from exc
    except Exception as exc:
        raise NotificationError(
            f"Unexpected error publishing failure notification: {type(exc).__name__}"
        ) from exc