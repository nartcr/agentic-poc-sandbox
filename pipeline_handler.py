# BOILERPLATE
import json
import logging
import os
from datetime import datetime

import pytz

import audit_writer
import db_loader
import error_writer
import file_parser
import report_builder
import row_validator
import sns_notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def lambda_handler(event: dict, context: object) -> dict:  # LOGIC
    """
    AWS Lambda entry point. Orchestrates the full positions pipeline:
    parse → validate → load → report → notify → audit.
    Returns {"statusCode": 200|500, "body": json.dumps(result_dict)}.
    """
    # BOILERPLATE — capture wall-clock invocation time in ET immediately
    invocation_time_et: datetime = datetime.now(_ET)

    # BOILERPLATE — read environment variables
    bucket: str = os.environ["S3_BUCKET"]
    db_secret_id: str = os.environ["DB_SECRET_ID"]

    # LOGIC — extract S3 trigger details from event
    record = event["Records"][0]
    bucket_from_event: str = record["s3"]["bucket"]["name"]
    object_key: str = record["s3"]["object"]["key"]

    logger.info(
        "Lambda invoked for bucket=%s key=%s at %s",
        bucket_from_event,
        object_key,
        invocation_time_et.isoformat(),
    )

    # LOGIC — mutable pipeline state, initialised to failure defaults
    desk_code: str | None = None
    trade_date: str | None = None
    filename: str = object_key
    rows_inserted: int = 0
    rows_rejected: int = 0
    total_rows: int = 0
    error_file_key: str | None = None
    report_s3_key: str | None = None
    status: str = "FAILURE"
    error_message: str | None = None
    conn = None

    try:
        # LOGIC — open DB connection once; reuse for loader and audit writer
        conn = db_loader.get_db_connection(db_secret_id)

        # LOGIC — step 1: download CSV and parse filename metadata
        raw_df, metadata = file_parser.download_and_parse(bucket_from_event, object_key)
        desk_code = metadata["desk_code"]
        trade_date = metadata["trade_date"]
        filename = metadata["filename"]
        total_rows = len(raw_df)

        logger.info(
            "Parsed filename=%s desk_code=%s trade_date=%s total_rows=%d",
            filename,
            desk_code,
            trade_date,
            total_rows,
        )

        # LOGIC — step 2: validate rows
        valid_df, rejected_df = row_validator.validate_rows(raw_df)
        rows_rejected = len(rejected_df)

        logger.info(
            "Validation complete: valid_rows=%d rejected_rows=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — step 3: write rejected rows to S3 error file (even if empty,
        # only write when there are rejections to avoid noise)
        if rows_rejected > 0:
            error_file_key = error_writer.write_error_file(
                rejected_df=rejected_df,
                bucket=bucket,
                desk_code=desk_code,
                trade_date=trade_date,
            )
            logger.info("Error file written to s3://%s/%s", bucket, error_file_key)

        # LOGIC — step 4: load valid rows into DB
        rows_inserted = db_loader.load_positions(conn, valid_df)

        logger.info(
            "DB load complete: rows_inserted=%d rows_skipped=%d",
            rows_inserted,
            len(valid_df) - rows_inserted,
        )

        # LOGIC — step 5: build and upload summary report
        summary: dict = report_builder.build_and_upload_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code,
            trade_date=trade_date,
            filename=filename,
            bucket=bucket,
        )
        report_s3_key = summary.get("report_s3_key")

        logger.info("Report written to s3://%s/%s", bucket, report_s3_key)

        # LOGIC — determine final status
        if rows_rejected == 0:
            status = "SUCCESS"
        else:
            status = "PARTIAL"

        # LOGIC — step 6: notify success via SNS
        sns_notifier.notify_success(
            summary=summary,
            report_s3_key=report_s3_key,
        )

    except Exception as exc:  # LOGIC — catch-all for failure path
        status = "FAILURE"
        error_message = str(exc)[:2000]
        logger.exception(
            "Pipeline failed for key=%s: %s", object_key, error_message
        )

        # LOGIC — send failure notification; must not raise
        sns_notifier.notify_failure(
            filename=filename,
            desk_code=desk_code,
            trade_date=trade_date,
            error_message=error_message,
            timestamp_et=invocation_time_et,
        )

    finally:
        # LOGIC — always write audit record regardless of outcome
        if conn is not None:
            try:
                audit_writer.write_audit_record(
                    conn=conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    status=status,
                    total_rows=total_rows,
                    rows_inserted=rows_inserted,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                    processing_timestamp_et=invocation_time_et,
                )
                logger.info("Audit record written: status=%s", status)
            except Exception as audit_exc:  # BOILERPLATE — never let audit block response
                logger.warning(
                    "Failed to write audit record: %s", str(audit_exc)
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        else:
            logger.warning(
                "DB connection was never established; audit record not written."
            )

    # LOGIC — build structured response body
    if status == "FAILURE":
        response_status_code = 500
    else:
        response_status_code = 200

    result_dict = {
        "status": status,
        "filename": filename,
        "desk_code": desk_code,
        "trade_date": trade_date,
        "total_rows": total_rows,
        "rows_inserted": rows_inserted,
        "rows_rejected": rows_rejected,
        "error_file_key": error_file_key,
        "report_s3_key": report_s3_key,
        "error_message": error_message,
    }

    logger.info("Lambda returning statusCode=%d body=%s", response_status_code, result_dict)

    return {
        "statusCode": response_status_code,
        "body": json.dumps(result_dict),
    }