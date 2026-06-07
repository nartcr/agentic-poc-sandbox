# BOILERPLATE
import json
import logging
from urllib.parse import unquote_plus

import boto3
import pytz

import src.config as config
import src.db as db
import src.pipeline as pipeline

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context) -> dict:
    # BOILERPLATE — parse the S3 trigger event; S3 encodes special characters in object keys
    try:
        record = event["Records"][0]
        bucket_name: str = record["s3"]["bucket"]["name"]
        raw_key: str = record["s3"]["object"]["key"]
        s3_key: str = unquote_plus(raw_key)
    except (KeyError, IndexError) as parse_exc:
        logger.error("handler: failed to parse S3 event record error=%s", str(parse_exc))
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Malformed S3 event: {str(parse_exc)}"}),
        }

    logger.info(
        "handler: received S3 event bucket=%s key=%s", bucket_name, s3_key
    )

    # LOGIC — only process files matching the expected naming pattern; skip all others
    if not s3_key.endswith("_positions.csv"):
        logger.info(
            "handler: skipping key=%s — does not match _positions.csv pattern", s3_key
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Skipped non-position file: {s3_key}"}),
        }

    # BOILERPLATE — initialize AWS clients; region read from environment via config
    s3_client = boto3.client("s3", region_name=config.AWS_REGION)
    sns_client = boto3.client("sns", region_name=config.AWS_REGION)

    logger.info("handler: AWS clients initialized region=%s", config.AWS_REGION)

    # LOGIC — open DB connection via context manager; connection is always closed on exit
    try:
        with db.get_connection() as conn:
            logger.info("handler: DB connection established")
            report = pipeline.process_file(
                s3_key=s3_key,
                s3_client=s3_client,
                sns_client=sns_client,
                db_conn=conn,
            )
            logger.info(
                "handler: pipeline completed successfully desk_code=%s trade_date=%s",
                report.get("desk_code", ""),
                report.get("trade_date", ""),
            )
            return {
                "statusCode": 200,
                "body": json.dumps(report),
            }

    except Exception as exc:
        logger.error(
            "handler: pipeline failed for key=%s error=%s", s3_key, str(exc), exc_info=True
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(exc)}),
        }