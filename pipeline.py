import logging  # BOILERPLATE
from datetime import datetime  # BOILERPLATE
from typing import Optional  # BOILERPLATE

import pytz  # BOILERPLATE

import s3_reader  # BOILERPLATE
import validator  # BOILERPLATE
import error_writer  # BOILERPLATE
import loader  # BOILERPLATE
import reporter  # BOILERPLATE
import notifier  # BOILERPLATE
import audit  # BOILERPLATE
import secrets as app_secrets  # BOILERPLATE — renamed to avoid shadowing stdlib secrets
import db  # BOILERPLATE

from config import Config  # BOILERPLATE

logger = logging.getLogger(__name__)  # BOILERPLATE

_ET = pytz.timezone("America/Toronto")  # BOILERPLATE


def process_file(
    s3_key: str,
    config: Config,
    service_identity: str = "unknown/unknown",
) -> dict:
    # LOGIC — captures processing timestamp once; all downstream components use this value
    processing_timestamp: datetime = datetime.now(_ET)

    logger.info(
        "Starting process_file: s3_key=%s service_identity=%s ts=%s",
        s3_key,
        service_identity,
        processing_timestamp.isoformat(),
    )

    # LOGIC — Step 1 & 2: read CSV from S3 and parse desk_code / trade_date from filename
    raw_df, desk_code, trade_date = s3_reader.read_csv_from_s3(
        config.s3_bucket, s3_key
    )
    logger.info(
        "Read %d rows from s3://%s/%s (desk_code=%s trade_date=%s)",
        len(raw_df),
        config.s3_bucket,
        s3_key,
        desk_code,
        trade_date,
    )

    # LOGIC — Step 3: validate rows; separates valid_df and rejected_df
    valid_df, rejected_df = validator.validate_rows(raw_df, desk_code, trade_date)
    logger.info(
        "Validation complete: valid=%d rejected=%d",
        len(valid_df),
        len(rejected_df),
    )

    # LOGIC — Step 4: write error file only when there are rejected rows
    error_s3_key: Optional[str] = None
    if len(rejected_df) > 0:
        error_s3_key = error_writer.write_error_file(
            rejected_df=rejected_df,
            bucket=config.s3_bucket,
            error_prefix=config.s3_error_prefix,
            desk_code=desk_code,
            trade_date=trade_date,
            processing_timestamp=processing_timestamp,
        )
        logger.info("Error file written: s3://%s/%s", config.s3_bucket, error_s3_key)

    # LOGIC — Steps 5–10: open DB connection, load, report, audit, commit — all in one transaction
    conn = None
    rows_inserted: int = 0
    report: Optional[dict] = None
    report_s3_key: Optional[str] = None

    try:
        credentials = app_secrets.get_db_credentials(config.db_secret_id)

        with db.get_connection(credentials) as conn:
            try:
                # LOGIC — Step 7: load valid rows; ON CONFLICT DO NOTHING handles duplicates
                rows_inserted = loader.load_positions(valid_df, conn)
                logger.info(
                    "Loaded %d rows into demo_schema.trade_positions "
                    "(skipped %d duplicates)",
                    rows_inserted,
                    len(valid_df) - rows_inserted,
                )

                # LOGIC — Step 8: build and write summary report to S3
                report = reporter.build_report(
                    raw_df=raw_df,
                    valid_df=valid_df,
                    rejected_df=rejected_df,
                    rows_inserted=rows_inserted,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    processing_timestamp=processing_timestamp,
                    source_s3_key=s3_key,
                )
                # LOGIC — inject error_file_s3_key into report before writing
                report["error_file_s3_key"] = error_s3_key

                report_s3_key = reporter.write_report(
                    report=report,
                    bucket=config.s3_bucket,
                    report_prefix=config.s3_report_prefix,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    processing_timestamp=processing_timestamp,
                )
                logger.info(
                    "Report written: s3://%s/%s", config.s3_bucket, report_s3_key
                )
                # LOGIC — inject report_s3_key into report dict for SNS and return value
                report["report_s3_key"] = report_s3_key

                # LOGIC — Step 9: determine audit status
                if len(rejected_df) == 0:
                    audit_status = "SUCCESS"
                else:
                    audit_status = "PARTIAL"

                audit.record_audit(
                    conn=conn,
                    desk_code=desk_code,
                    trade_date=trade_date,
                    source_s3_key=s3_key,
                    status=audit_status,
                    total_rows=len(raw_df),
                    rows_inserted=rows_inserted,
                    rows_rejected=len(rejected_df),
                    error_message=None,
                    processing_timestamp=processing_timestamp,
                    service_identity=service_identity,
                )

                # LOGIC — Step 10: commit transaction — load + audit are atomic
                conn.commit()
                logger.info(
                    "Transaction committed: desk_code=%s trade_date=%s status=%s",
                    desk_code,
                    trade_date,
                    audit_status,
                )

            except Exception as db_exc:
                # LOGIC — rollback on any DB-layer exception before re-raising
                try:
                    conn.rollback()
                    logger.warning(
                        "Transaction rolled back due to exception: %s", db_exc
                    )
                except Exception as rb_exc:
                    logger.error("Rollback failed: %s", rb_exc)
                raise

    except Exception as exc:
        # LOGIC — failure path: attempt to write audit record, publish failure SNS, re-raise
        logger.error(
            "process_file failed: desk_code=%s trade_date=%s error=%s",
            desk_code,
            trade_date,
            exc,
        )

        # LOGIC — attempt failure audit in a separate connection so a DB error above
        #         doesn't prevent us from recording the FAILURE status
        _write_failure_audit(
            config=config,
            desk_code=desk_code,
            trade_date=trade_date,
            s3_key=s3_key,
            raw_row_count=len(raw_df) if raw_df is not None else 0,
            rows_inserted=rows_inserted,
            rows_rejected=len(rejected_df) if rejected_df is not None else 0,
            error_message=_sanitize_error(str(exc)),
            processing_timestamp=processing_timestamp,
            service_identity=service_identity,
        )

        # LOGIC — Step 12 (failure): notify failure topic; errors are logged, not re-raised
        notifier.notify_failure(
            topic_arn=config.sns_failure_topic_arn,
            desk_code=desk_code,
            trade_date=trade_date,
            error_message=_sanitize_error(str(exc)),
            processing_timestamp=processing_timestamp,
        )

        raise

    # LOGIC — Step 11: success notification (outside the DB context manager)
    notifier.notify_success(
        topic_arn=config.sns_success_topic_arn,
        report=report,
    )
    logger.info(
        "process_file complete: desk_code=%s trade_date=%s rows_inserted=%d",
        desk_code,
        trade_date,
        rows_inserted,
    )

    return report


def _write_failure_audit(
    config: Config,
    desk_code: str,
    trade_date: str,
    s3_key: str,
    raw_row_count: int,
    rows_inserted: int,
    rows_rejected: int,
    error_message: str,
    processing_timestamp: datetime,
    service_identity: str,
) -> None:
    # LOGIC — opens a fresh connection for the failure audit so a rolled-back transaction
    #         does not prevent the audit row from being persisted
    try:
        credentials = app_secrets.get_db_credentials(config.db_secret_id)
        with db.get_connection(credentials) as conn:
            audit.record_audit(
                conn=conn,
                desk_code=desk_code,
                trade_date=trade_date,
                source_s3_key=s3_key,
                status="FAILURE",
                total_rows=raw_row_count,
                rows_inserted=rows_inserted,
                rows_rejected=rows_rejected,
                error_message=error_message,
                processing_timestamp=processing_timestamp,
                service_identity=service_identity,
            )
            conn.commit()
            logger.info(
                "Failure audit record committed: desk_code=%s trade_date=%s",
                desk_code,
                trade_date,
            )
    except Exception as audit_exc:
        # LOGIC — audit failure must not mask the original exception
        logger.error(
            "Failed to write failure audit record for desk_code=%s trade_date=%s: %s",
            desk_code,
            trade_date,
            audit_exc,
        )


def _sanitize_error(error_message: str) -> str:
    # LOGIC — strips credential-like substrings from error messages before logging or SNS publish
    #         Credential values are never present in exceptions from secrets.py (it sanitizes),
    #         but we apply a belt-and-suspenders scrub here anyway.
    sanitized = error_message
    # Remove any occurrence of "password" key-value patterns (conservative scrub)
    import re  # BOILERPLATE — local import to keep module-level imports minimal
    sanitized = re.sub(
        r"password['\"]?\s*[:=]\s*['\"]?[^\s,}\"']+",
        "password=<redacted>",
        sanitized,
        flags=re.IGNORECASE,
    )
    return sanitized