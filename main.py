# BOILERPLATE
import logging

from pipeline import run_pipeline

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:  # LOGIC — Lambda entry point
    """
    AWS Lambda entry point. Extracts the S3 key from the S3-triggered event
    and delegates to the pipeline.
    """
    logger.info("lambda_handler invoked. event=%s", event)

    # LOGIC — extract S3 key from Lambda S3-trigger event structure
    try:
        s3_key: str = event["Records"][0]["s3"]["object"]["key"]
    except (KeyError, IndexError, TypeError) as exc:
        logger.error("Could not extract S3 key from event: %s", str(exc))
        return {"statusCode": 400, "body": f"Invalid event structure: {exc}"}

    logger.info("Processing s3_key=%s", s3_key)

    # LOGIC — run the pipeline; catch and surface any unhandled exception as HTTP 500
    try:
        run_pipeline(s3_key)
        logger.info("Pipeline completed successfully. s3_key=%s", s3_key)
        return {"statusCode": 200, "body": "OK"}
    except Exception as exc:
        logger.error("Pipeline failed. s3_key=%s error=%s", s3_key, str(exc))
        return {"statusCode": 500, "body": str(exc)}