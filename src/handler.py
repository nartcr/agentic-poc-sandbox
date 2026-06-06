# BOILERPLATE
import json
import logging
import os
from urllib.parse import unquote_plus

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — import pipeline at module level so cold-start errors surface immediately
from src import pipeline


# LOGIC
def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda entry point. Parses S3 event trigger records and invokes
    pipeline.run_pipeline for each valid incoming positions file.
    Returns 200 if all records succeed, 207 if any fail.
    """
    logger.info("Lambda invoked. Number of records: %d", len(event.get("Records", [])))

    processed = []
    failed = []

    # LOGIC — iterate over all S3 event records
    for record in event.get("Records", []):
        try:
            bucket_name = record["s3"]["bucket"]["name"]
            raw_key = record["s3"]["object"]["key"]
            # LOGIC — URL-decode the key (S3 events percent-encode special characters)
            s3_key = unquote_plus(raw_key)
        except (KeyError, TypeError) as exc:
            logger.error("Malformed S3 event record — cannot extract bucket/key: %s", exc)
            failed.append({"key": None, "error": str(exc)})
            continue

        # LOGIC — filter: only process keys under incoming/ that end with _positions.csv
        if not s3_key.startswith("incoming/") or not s3_key.endswith("_positions.csv"):
            logger.warning(
                "Skipping S3 key '%s': does not match incoming/*_positions.csv pattern.",
                s3_key,
            )
            continue

        logger.info("Processing S3 key: %s (bucket: %s)", s3_key, bucket_name)

        try:
            # LOGIC — delegate all processing to the pipeline orchestrator
            report = pipeline.run_pipeline(s3_key)
            logger.info(
                "Pipeline completed for key '%s'. Status: %s, rows_inserted: %d",
                s3_key,
                report.get("status"),
                report.get("rows_inserted", 0),
            )
            processed.append(s3_key)
        except Exception as exc:  # LOGIC — catch-all: log, record failure, continue
            logger.error(
                "Pipeline failed for key '%s': %s",
                s3_key,
                exc,
                exc_info=True,
            )
            failed.append({"key": s3_key, "error": str(exc)})

    # LOGIC — determine response code: 207 Multi-Status if any failures, 200 if fully successful
    if failed:
        status_code = 207
        logger.warning(
            "Completed with partial failures. processed=%d, failed=%d",
            len(processed),
            len(failed),
        )
        return {
            "statusCode": status_code,
            "processed": processed,
            "failed": failed,
        }

    logger.info("All records processed successfully. processed=%d", len(processed))
    return {
        "statusCode": 200,
        "processed": processed,
    }