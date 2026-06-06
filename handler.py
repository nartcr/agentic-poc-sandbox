import logging
from datetime import datetime
from urllib.parse import unquote_plus

import pytz  # BOILERPLATE

import config
import secrets_manager
import s3_reader
import validator
import error_writer
import loader
import reporter
import audit
import notifier

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ET = pytz.timezone("America/Toronto")


def lambda_handler(event: dict, context: object) -> dict:
    # LOGIC
    """
    Entry point for Lambda invocation triggered by S3 ObjectCreated event.
    Iterates over event["Records"], extracting bucket and key for each record.
    For each record:
      1. Calls s3_reader.read_position_file(bucket, key)
      2. Calls validator.validate_rows(raw_bytes, desk_code, trade_date)
      3. Calls error_writer.write_error_file(...) if rejected_df non-empty
      4. Calls loader.load_positions(valid_df, db_credentials)
      5. Calls reporter.build_report(...) and reporter.write_report(...)
      6. Calls audit.write_audit_record(...)
      7. Calls notifier.notify_success(...) or notifier.notify_failure(...)
    Returns {"statusCode": 200, "processed": [list of processed s3 keys]}
    on full completion, even if some files were PARTIAL outcome.
    Returns {"statusCode": 500, "error": error_message} only if an
    unrecoverable top-level exception prevents all processing.
    """
    # BOILERPLATE — load config once per invocation
    try:
        cfg = config.Config()
    except EnvironmentError as exc:
        logger.error("handler: missing required environment variable: %s", exc)
        return {"statusCode": 500, "error": str(exc)}

    # BOILERPLATE — fetch DB credentials once per invocation (cached by secrets_manager)
    try:
        db_credentials = secrets_manager.get_db_credentials(cfg.db_secret_id)
    except RuntimeError as exc:
        logger.error("handler: failed to retrieve DB credentials: %s", exc)
        return {"statusCode": 500, "error": str(exc)}

    records = event.get("Records", [])
    if not records:
        logger.warning("handler: event contained no Records")
        return {"statusCode": 200, "processed": []}

    processed_keys = []

    for record in records:
        # LOGIC — extract bucket and key from S3 event record
        try:
            bucket = record["s3"]["bucket"]["name"]
            raw_key = record["s3"]["object"]["key"]
            key = unquote_plus(raw_key)  # S3 event keys are URL-encoded
        except KeyError as exc:
            logger.error("handler: malformed S3 event record, missing field %s", exc)
            continue

        # LOGIC — set processing timestamp once per file in ET
        processing_timestamp = datetime.now(_ET)

        logger.info(
            "handler: starting processing for s3://%s/%s at %s",
            bucket,
            key,
            processing_timestamp.isoformat(),
        )

        # LOGIC — per-file pipeline with full exception isolation
        try:
            _process_single_file(
                bucket=bucket,
                key=key,
                cfg=cfg,
                db_credentials=db_credentials,
                processing_timestamp=processing_timestamp,
            )
            processed_keys.append(key)

        except Exception as exc:  # LOGIC — top-level catch: write failure audit + notify
            logger.error(
                "handler: unrecoverable error processing %s: %s",
                key,
                exc,
                exc_info=True,
            )
            # LOGIC — best-effort failure audit record
            try:
                audit.write_audit_record(
                    db_credentials=db_credentials,
                    s3_key=key,
                    desk_code="UNKNOWN",
                    trade_date="UNKNOWN",
                    processing_timestamp=processing_timestamp,
                    outcome="FAILURE",
                    total_rows=0,
                    rows_loaded=0,
                    rows_rejected=0,
                    rows_skipped=0,
                    error_message=str(exc),
                    report_s3_key=None,
                    error_file_s3_key=None,
                    service_identity=cfg.pipeline_service_identity,
                )
            except Exception as audit_exc:
                logger.error(
                    "handler: failed to write failure audit record for %s: %s",
                    key,
                    audit_exc,
                )

            # LOGIC — best-effort failure notification
            try:
                notifier.notify_failure(
                    topic_arn=cfg.sns_failure_topic_arn,
                    s3_key=key,
                    error_message=str(exc),
                    processing_timestamp=processing_timestamp,
                )
            except Exception as notify_exc:
                logger.error(
                    "handler: failed to send failure notification for %s: %s",
                    key,
                    notify_exc,
                )

            # LOGIC — file-level failure does not abort processing of other files
            processed_keys.append(key)

    return {"statusCode": 200, "processed": processed_keys}


def _process_single_file(
    bucket: str,
    key: str,
    cfg: "config.Config",
    db_credentials: dict,
    processing_timestamp: datetime,
) -> None:
    # LOGIC — step 1: read file from S3 and extract filename metadata
    raw_bytes, metadata = s3_reader.read_position_file(bucket, key)
    desk_code = metadata["desk_code"]
    trade_date = metadata["trade_date"]

    logger.info(
        "_process_single_file: desk_code=%s, trade_date=%s, key=%s",
        desk_code,
        trade_date,
        key,
    )

    # LOGIC — step 2: validate rows
    valid_df, rejected_df = validator.validate_rows(raw_bytes, desk_code, trade_date)

    # LOGIC — reconstruct raw_df for reporter (parse CSV to DataFrame without validation)
    import io
    import pandas as pd  # BOILERPLATE — local import to keep module top lean
    raw_df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, keep_default_na=False)

    logger.info(
        "_process_single_file: total=%d, valid=%d, rejected=%d",
        len(raw_df),
        len(valid_df),
        len(rejected_df),
    )

    # LOGIC — step 3: write error file if any rows were rejected
    error_file_s3_key = None
    if not rejected_df.empty:
        error_file_s3_key = error_writer.write_error_file(
            bucket=bucket,
            error_prefix=cfg.s3_error_prefix,
            rejected_df=rejected_df,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_timestamp=processing_timestamp,
        )
        logger.info(
            "_process_single_file: error file written to %s", error_file_s3_key
        )

    # LOGIC — step 4: load valid rows to Aurora
    rows_loaded = loader.load_positions(valid_df, db_credentials)

    logger.info(
        "_process_single_file: rows_loaded=%d (net new inserts)", rows_loaded
    )

    # LOGIC — step 5a: build summary report
    report = reporter.build_report(
        s3_key=key,
        desk_code=desk_code,
        trade_date=trade_date,
        raw_df=raw_df,
        valid_df=valid_df,
        rejected_df=rejected_df,
        rows_loaded=rows_loaded,
        processing_timestamp=processing_timestamp,
        error_file_s3_key=error_file_s3_key,
    )

    # LOGIC — step 5b: write report to S3
    report_s3_key = reporter.write_report(
        bucket=bucket,
        report_prefix=cfg.s3_report_prefix,
        report=report,
        desk_code=desk_code,
        trade_date=trade_date,
        processing_timestamp=processing_timestamp,
    )

    logger.info(
        "_process_single_file: report written to %s", report_s3_key
    )

    # LOGIC — step 6: determine outcome
    total_rows = len(raw_df)
    rows_rejected = len(rejected_df)
    rows_skipped = len(valid_df) - rows_loaded

    if rows_rejected == 0 and rows_loaded > 0:
        outcome = "SUCCESS"
    elif rows_rejected > 0 and rows_loaded > 0:
        outcome = "PARTIAL"
    else:
        # rows_loaded == 0 and total_rows > 0, or all rows rejected
        outcome = "FAILURE"

    logger.info(
        "_process_single_file: outcome=%s, total=%d, loaded=%d, rejected=%d, skipped=%d",
        outcome,
        total_rows,
        rows_loaded,
        rows_rejected,
        rows_skipped,
    )

    # LOGIC — step 6: write audit record
    audit.write_audit_record(
        db_credentials=db_credentials,
        s3_key=key,
        desk_code=desk_code,
        trade_date=trade_date,
        processing_timestamp=processing_timestamp,
        outcome=outcome,
        total_rows=total_rows,
        rows_loaded=rows_loaded,
        rows_rejected=rows_rejected,
        rows_skipped=rows_skipped,
        error_message=None,
        report_s3_key=report_s3_key,
        error_file_s3_key=error_file_s3_key,
        service_identity=cfg.pipeline_service_identity,
    )

    # LOGIC — step 7: notify based on outcome
    if outcome in ("SUCCESS", "PARTIAL"):
        notifier.notify_success(
            topic_arn=cfg.sns_success_topic_arn,
            report=report,
        )
    else:
        notifier.notify_failure(
            topic_arn=cfg.sns_failure_topic_arn,
            s3_key=key,
            error_message=(
                f"No rows loaded: total_rows={total_rows}, "
                f"rows_rejected={rows_rejected}, rows_loaded={rows_loaded}"
            ),
            processing_timestamp=processing_timestamp,
        )