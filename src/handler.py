# BOILERPLATE
import json
import logging
import os

from src.config import Config
from src import pipeline

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:  # LOGIC
    """
    AWS Lambda entry point. Receives an S3 event notification (object created),
    extracts the S3 key from each record, and calls pipeline.run_pipeline.
    Returns HTTP 200 on full success, 500 if any record fails.
    """
    # BOILERPLATE — load config once; raises EnvironmentError if any var missing
    cfg = Config.from_env()

    records = event.get("Records", [])
    if not records:
        logger.warning("lambda_handler: event contained no Records; nothing to process")
        return {"statusCode": 200, "body": "OK"}

    encountered_failure = False
    last_exception: Exception | None = None

    # LOGIC — iterate over all S3 records in the event
    for record in records:
        s3_key: str | None = None
        try:
            s3_key = record["s3"]["object"]["key"]
            logger.info("lambda_handler: processing s3_key=%s", s3_key)
            pipeline.run_pipeline(s3_key=s3_key, cfg=cfg)
            logger.info("lambda_handler: completed successfully for s3_key=%s", s3_key)
        except Exception as exc:  # LOGIC — per-record failure: log, continue, flag
            logger.error(
                "lambda_handler: unhandled exception for s3_key=%s error=%s",
                s3_key,
                str(exc),
                exc_info=True,
            )
            encountered_failure = True
            last_exception = exc

    # LOGIC — return 500 if any record failed so S3 can retry the invocation
    if encountered_failure:
        return {"statusCode": 500, "body": str(last_exception)}

    return {"statusCode": 200, "body": "OK"}