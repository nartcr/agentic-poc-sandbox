# BOILERPLATE
import json
import logging
import os
import re
import urllib.parse

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — filename pattern from data contract
_KEY_PATTERN = re.compile(r'^[A-Z0-9]+_\d{4}-\d{2}-\d{2}_positions\.csv$')


def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point. Parses the S3 trigger event, validates the
    incoming key format, and delegates processing to run_pipeline().
    """
    # BOILERPLATE — lazy import so the module is only loaded inside Lambda
    from src.pipeline import run_pipeline  # BOILERPLATE

    s3_key = None
    try:
        # LOGIC — extract and URL-decode the S3 object key
        raw_key = event["Records"][0]["s3"]["object"]["key"]
        s3_key = urllib.parse.unquote_plus(raw_key)
        logger.info("Lambda triggered for S3 key: %s", s3_key)

        # LOGIC — validate that the filename (not the full prefix path) matches
        # the expected pattern: {DESK_CODE}_{YYYY-MM-DD}_positions.csv
        filename = os.path.basename(s3_key)
        if not _KEY_PATTERN.match(filename):
            raise ValueError(f"Unexpected file key format: {s3_key}")

        # LOGIC — delegate to pipeline orchestrator
        result = run_pipeline(s3_key)

        # BOILERPLATE — success response
        response_body = json.dumps({
            "pipeline_run": result["source_file"],
            "rows_loaded": result["rows_loaded"],
        })
        logger.info(
            "Pipeline completed successfully. source_file=%s rows_loaded=%d",
            result["source_file"],
            result["rows_loaded"],
        )
        return {"statusCode": 200, "body": response_body}

    except Exception as e:  # LOGIC — surface any error as a 500 response
        logger.exception(
            "Pipeline failed for key=%s error=%s", s3_key, str(e)
        )
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }