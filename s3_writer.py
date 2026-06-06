# BOILERPLATE
import io
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_error_file(rejected_df, desk_code: str, trade_date: str) -> str:
    # LOGIC — serialize rejected rows (including rejection_reason column) to CSV and write to S3
    bucket = os.environ["S3_BUCKET"]
    key = f"errors/{desk_code}_{trade_date}_errors.csv"

    # LOGIC — serialize DataFrame to CSV bytes in memory; no /tmp/ paths used
    csv_buffer = io.StringIO()
    rejected_df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # BOILERPLATE — boto3 S3 client, no credentials in code
    s3_client = boto3.client("s3")

    # LOGIC — write error CSV to the errors/ prefix under the configured bucket
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_bytes,
        ContentType="text/csv",
    )

    logger.info(
        "Error file written to s3://%s/%s (%d rejected rows)",
        bucket,
        key,
        len(rejected_df),
    )

    return key


def write_report(summary: dict, desk_code: str, trade_date: str) -> str:
    # LOGIC — serialize summary dict to JSON and write to S3 reports/ prefix
    bucket = os.environ["S3_BUCKET"]
    key = f"reports/{desk_code}_{trade_date}_summary.json"

    # LOGIC — JSON with indent=2 as specified in the data contracts
    json_bytes = json.dumps(summary, indent=2).encode("utf-8")

    # BOILERPLATE — boto3 S3 client, no credentials in code
    s3_client = boto3.client("s3")

    # LOGIC — write summary JSON to the reports/ prefix under the configured bucket
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_bytes,
        ContentType="application/json",
    )

    logger.info(
        "Summary report written to s3://%s/%s",
        bucket,
        key,
    )

    return key