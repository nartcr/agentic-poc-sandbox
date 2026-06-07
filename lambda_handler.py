# BOILERPLATE
import logging
import os

import db_connection
import file_reader
import row_validator
import db_loader
import error_writer
import report_builder
import sns_notifier
import audit_writer

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event: dict, context: object) -> dict:
    """
    Entry point for the AWS Lambda function.
    Orchestrates: read → validate → load → report → notify → audit.
    """
    # BOILERPLATE — extract S3 event metadata
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]
    filename = key  # full S3 key used as filename in audit

    logger.info("Handler invoked. bucket=%s key=%s", bucket, key)

    conn = None
    desk_code = None
    trade_date_str = None
    total_rows = 0
    rows_inserted = 0
    rows_rejected = 0
    status = "FAILURE"
    error_message = None
    report = {}

    try:
        # BOILERPLATE — open DB connection once for the full pipeline
        conn = db_connection.get_connection()

        # LOGIC — read CSV from S3 and parse filename metadata
        raw_df, desk_code, trade_date_str = file_reader.read_csv_from_s3(bucket, key)
        total_rows = len(raw_df)
        logger.info(
            "File read complete. desk_code=%s trade_date=%s total_rows=%d",
            desk_code,
            trade_date_str,
            total_rows,
        )

        # LOGIC — validate rows against all mandatory field and type rules
        valid_df, rejected_df = row_validator.validate_rows(raw_df, desk_code, trade_date_str)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete. valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — write rejected rows to S3 errors prefix (always, even if empty)
        error_writer.write_error_file(rejected_df, bucket, desk_code, trade_date_str)

        # LOGIC — load valid rows into demo_schema.trade_positions (idempotent upsert)
        rows_inserted = db_loader.load_positions(valid_df, conn)
        logger.info("DB load complete. rows_inserted=%d", rows_inserted)

        # LOGIC — build and publish the summary report to S3
        report = report_builder.build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=rows_inserted,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            bucket=bucket,
        )

        # LOGIC — determine pipeline status
        if rows_rejected == 0:
            status = "SUCCESS"
        else:
            status = "PARTIAL"

        logger.info("Pipeline status=%s", status)

        # LOGIC — notify downstream via SNS success topic (SUCCESS and PARTIAL both go here per TAC-5)
        sns_notifier.publish_success(report)

        # LOGIC — write audit record
        audit_writer.write_audit_record(
            conn=conn,
            filename=filename,
            desk_code=desk_code,
            trade_date_str=trade_date_str,
            status=status,
            total_rows=total_rows,
            rows_inserted=rows_inserted,
            rows_rejected=rows_rejected,
            error_message=None,
        )

        summary = (
            f"Processed {filename}: total={total_rows} "
            f"inserted={rows_inserted} rejected={rows_rejected} status={status}"
        )
        logger.info(summary)
        return {"statusCode": 200, "body": summary}

    except Exception as exc:  # LOGIC — catch-all for unhandled pipeline failures
        error_message = str(exc)
        status = "FAILURE"
        logger.exception(
            "Unhandled exception during pipeline execution. filename=%s error=%s",
            filename,
            error_message,
        )

        # LOGIC — publish failure notification to SNS failure topic
        try:
            sns_notifier.publish_failure(
                filename=filename,
                error_message=error_message,
                desk_code=desk_code,
                trade_date_str=trade_date_str,
            )
        except Exception as sns_exc:
            logger.error("SNS failure publish itself failed: %s", str(sns_exc))

        # LOGIC — write audit record even on failure (committed independently)
        if conn is not None:
            try:
                audit_writer.write_audit_record(
                    conn=conn,
                    filename=filename,
                    desk_code=desk_code,
                    trade_date_str=trade_date_str,
                    status="FAILURE",
                    total_rows=total_rows,
                    rows_inserted=rows_inserted,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                )
            except Exception as audit_exc:
                logger.error("Audit write on failure path failed: %s", str(audit_exc))

        return {"statusCode": 500, "body": error_message}

    finally:
        # BOILERPLATE — always close the DB connection
        if conn is not None:
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as close_exc:
                logger.error("Error closing database connection: %s", str(close_exc))