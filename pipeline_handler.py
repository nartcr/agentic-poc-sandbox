# BOILERPLATE
import json
import logging
from datetime import datetime

import boto3
import psycopg2
import pytz

import audit_logger
import db_loader
import error_file_writer
import report_builder
import row_validator
import s3_file_reader
import secret_reader
import sns_notifier
from pipeline_config import load_config

# BOILERPLATE — module-level logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# BOILERPLATE — ET timezone constant
_ET = pytz.timezone("America/Toronto")


def handler(event: dict, context) -> dict:
    # BOILERPLATE — load configuration from environment variables at startup
    config = load_config()

    # BOILERPLATE — instantiate AWS clients (use existing services only, never provision)
    s3_client = boto3.client("s3")
    sns_client = boto3.client("sns")

    # LOGIC — extract the S3 key from the Lambda S3 event notification
    s3_key: str = event["Records"][0]["s3"]["object"]["key"]
    logger.info("Pipeline triggered for S3 key: %s", s3_key)

    # LOGIC — parse desk_code and trade_date from the filename before any other work
    desk_code, trade_date = s3_file_reader.parse_filename_metadata(s3_key)
    logger.info("Parsed metadata: desk_code=%s trade_date=%s", desk_code, trade_date)

    # LOGIC — capture processing timestamp in ET as the authoritative pipeline start time
    processing_timestamp = datetime.now(_ET)
    logger.info("Processing timestamp (ET): %s", processing_timestamp.isoformat())

    # LOGIC — retrieve DB credentials from Secrets Manager at runtime (never from config)
    db_credentials = secret_reader.get_db_credentials(config.db_secret_id)

    # BOILERPLATE — open psycopg2 connection using credentials from Secrets Manager
    conn = psycopg2.connect(
        host=db_credentials["host"],
        port=int(db_credentials["port"]),
        user=db_credentials["username"],
        password=db_credentials["password"],
        dbname=db_credentials["dbname"],
    )

    # LOGIC — initialize pipeline state variables used in both success and failure paths
    raw_df = None
    valid_df = None
    rejected_df = None
    rows_inserted = 0
    report = None

    try:
        # LOGIC — read the raw CSV from S3; no type coercion at this stage
        logger.info("Reading position file from S3: bucket=%s key=%s", config.s3_bucket, s3_key)
        raw_df = s3_file_reader.read_position_file(s3_client, config.s3_bucket, s3_key)
        logger.info("Read %d rows from %s", len(raw_df), s3_key)

        # LOGIC — validate rows against all data quality rules; split into clean and rejected
        logger.info("Validating rows for desk_code=%s trade_date=%s", desk_code, trade_date)
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — write rejected rows to S3 error file (written even if rejected_df is empty)
        logger.info("Writing error file for desk_code=%s trade_date=%s", desk_code, trade_date)
        error_s3_key = error_file_writer.write_error_file(
            s3_client, config, rejected_df, desk_code, trade_date
        )
        logger.info("Error file written to: %s", error_s3_key)

        # LOGIC — bulk insert valid rows into demo_schema.trade_positions (idempotent via ON CONFLICT)
        logger.info("Loading %d valid rows into database", len(valid_df))
        rows_inserted = db_loader.load_positions(conn, valid_df)
        logger.info("Rows inserted into trade_positions: %d", rows_inserted)

        # LOGIC — build processing summary report and upload to S3 reports prefix
        logger.info("Building and uploading processing report")
        report = report_builder.build_and_upload_report(
            s3_client=s3_client,
            config=config,
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            processing_timestamp=processing_timestamp,
            desk_code=desk_code,
            trade_date=trade_date,
        )
        logger.info("Report uploaded: %s", report.get("report_s3_key"))

        # LOGIC — write SUCCESS audit record to demo_schema.pipeline_audit (idempotent upsert)
        audit_logger.write_audit_record(
            conn=conn,
            s3_key=s3_key,
            desk_code=desk_code,
            trade_date=trade_date,
            status="SUCCESS",
            total_rows=len(raw_df),
            rows_loaded=rows_inserted,
            rows_rejected=len(rejected_df),
            rows_skipped_dedup=len(valid_df) - rows_inserted,
            processing_timestamp_et=processing_timestamp,
            service_name=config.pipeline_service_name,
            error_message=None,
        )
        logger.info("Audit record written with status=SUCCESS for s3_key=%s", s3_key)

        # LOGIC — publish success notification to SNS success topic
        message_id = sns_notifier.notify_success(sns_client, config, report)
        logger.info("Success SNS notification sent. MessageId=%s", message_id)

        # BOILERPLATE — return HTTP 200 response on successful Lambda invocation
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "status": "SUCCESS",
                    "s3_key": s3_key,
                    "desk_code": desk_code,
                    "trade_date": trade_date.isoformat(),
                    "rows_inserted": rows_inserted,
                    "rows_rejected": len(rejected_df),
                }
            ),
        }

    except Exception as exc:
        # LOGIC — top-level failure handler: guarantee audit record and failure SNS are always written
        error_message = f"{type(exc).__name__}: {exc}"
        logger.exception("Pipeline failed for s3_key=%s: %s", s3_key, error_message)

        # LOGIC — compute safe row counts for the audit record using whatever state is available
        total_rows = len(raw_df) if raw_df is not None else 0
        rows_rejected_count = len(rejected_df) if rejected_df is not None else 0
        rows_valid_count = len(valid_df) if valid_df is not None else 0
        rows_skipped = rows_valid_count - rows_inserted

        # LOGIC — write FAILURE audit record; uses ON CONFLICT DO UPDATE so it is idempotent
        try:
            audit_logger.write_audit_record(
                conn=conn,
                s3_key=s3_key,
                desk_code=desk_code,
                trade_date=trade_date,
                status="FAILURE",
                total_rows=total_rows,
                rows_loaded=rows_inserted,
                rows_rejected=rows_rejected_count,
                rows_skipped_dedup=rows_skipped,
                processing_timestamp_et=processing_timestamp,
                service_name=config.pipeline_service_name,
                error_message=error_message,
            )
            logger.info("Audit record written with status=FAILURE for s3_key=%s", s3_key)
        except Exception as audit_exc:
            # BOILERPLATE — log but do not suppress; the original exception takes priority
            logger.exception(
                "Failed to write FAILURE audit record for s3_key=%s: %s",
                s3_key,
                audit_exc,
            )

        # LOGIC — publish failure notification to SNS failure topic
        try:
            trade_date_str = trade_date.isoformat() if trade_date is not None else "UNKNOWN"
            sns_notifier.notify_failure(
                sns_client=sns_client,
                config=config,
                desk_code=desk_code,
                trade_date=trade_date_str,
                s3_key=s3_key,
                error_message=error_message,
                partial_report=report,
            )
            logger.info("Failure SNS notification sent for s3_key=%s", s3_key)
        except Exception as sns_exc:
            # BOILERPLATE — log but do not suppress; the original exception takes priority
            logger.exception(
                "Failed to send failure SNS notification for s3_key=%s: %s",
                s3_key,
                sns_exc,
            )

        # LOGIC — re-raise the original exception so Lambda marks this invocation as failed
        raise

    finally:
        # BOILERPLATE — always close the database connection to prevent connection leaks
        try:
            conn.close()
            logger.info("Database connection closed for s3_key=%s", s3_key)
        except Exception as close_exc:
            logger.exception("Error closing database connection: %s", close_exc)