import logging
import os
import re
import sys

# BOILERPLATE
import pytz

from src.config import config
from src import s3_client
from src import pipeline

# BOILERPLATE
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# LOGIC
FILENAME_PATTERN = re.compile(r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$")


def main() -> int:
    # LOGIC
    bucket = config.s3_bucket
    prefix = config.input_prefix  # "incoming/"

    logger.info("Listing objects in bucket=%s prefix=%s", bucket, prefix)

    try:
        all_keys = s3_client.list_objects(bucket, prefix)
    except Exception as exc:  # LOGIC
        logger.error("Failed to list S3 objects: %s", exc)
        return 1

    # LOGIC — filter keys whose filename component matches the convention
    matching_keys = []
    for key in all_keys:
        filename = key.split("/")[-1]
        if FILENAME_PATTERN.match(filename):
            matching_keys.append(key)
        else:
            logger.debug("Skipping non-matching key: %s", key)

    total_found = len(matching_keys)
    logger.info("Total matching files found: %d", total_found)

    if total_found == 0:
        logger.info("No position files to process. Exiting.")
        return 0

    # LOGIC — process each file; collect failures but continue
    failed_keys = []
    processed_count = 0

    for s3_key in sorted(matching_keys):  # deterministic ordering
        logger.info("Processing file: %s", s3_key)
        try:
            pipeline.process_file(s3_key, config)
            processed_count += 1
            logger.info("Successfully processed: %s", s3_key)
        except Exception as exc:  # LOGIC
            logger.error("Failed to process file %s: %s", s3_key, exc)
            failed_keys.append(s3_key)

    # LOGIC — summary log
    logger.info(
        "Processing complete. Total=%d Succeeded=%d Failed=%d",
        total_found,
        processed_count,
        len(failed_keys),
    )

    if failed_keys:
        for fk in failed_keys:
            logger.error("File did not complete successfully: %s", fk)
        return 1

    return 0


if __name__ == "__main__":  # BOILERPLATE
    sys.exit(main())