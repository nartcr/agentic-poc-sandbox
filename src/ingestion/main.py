import logging
import os
import json
from datetime import datetime

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")  # BOILERPLATE


def _parse_s3_key(s3_key: str) -> tuple:
    # LOGIC — derives desk_code and trade_date from key pattern inbound/{desk_code}_{trade_date}_positions.csv
    basename = os.path.basename(s3_key)  # e.g. "EQTY_2026-06-15_positions.csv"
    if not basename.endswith("_positions.csv"):
        raise ValueError(
            f"S3 key '{s3_key}' does not match expected pattern "
            "'{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    stem = basename[: -len("_positions.csv")]  # e.g. "EQTY_2026-06-15"
    # desk_code has no underscores; trade_date is YYYY-MM-DD
    # Split on first underscore: everything before is desk_code, rest is trade_date
    parts = stem.split("_", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Cannot parse desk_code and trade_date from S3 key '{s3_key}'"
        )
    desk_code, trade_date = parts[0], parts[1]
    return desk_code, trade_date


def handler(event: dict, context: object) -> dict:
    # BOILERPLATE — Lambda entry point; lazy imports keep cold-start predictable
    from src.ingestion import audit, file_reader, validator, error_writer, loader, reporter, notifier

    # LOGIC — extract S3 event fields
    record = event["Records"][0]
    s3_bucket = record["s3"]["bucket"]["name"]
    s3_key = record["s3"]["object"]["key"]

    logger.info(
        "Pipeline started | bucket=%s | key=%s | timestamp=%s",
        s3_bucket,
        s3_key,
        datetime.now(ET).isoformat(),
    )

    # LOGIC — derive identifiers from filename
    desk_code, trade_date = _parse_s3_key(s3_key)

    audit_id = None  # LOGIC — guard so except block can safely reference it
    rows_inserted = 0
    rejected_count = 0

    try:
        # LOGIC — open audit record
        audit_id = audit.start_audit_record(s3_key, desk_code, trade_date)
        logger.info("Audit record created | audit_id=%s", audit_id)

        # LOGIC — read raw file from S3
        raw_df = file_reader.read_csv_from_s3(s3_bucket, s3_key)
        logger.info("File read | rows=%d | columns=%s", len(raw_df), list(raw_df.columns))

        # LOGIC — validate rows; split into valid/rejected
        valid_df, rejected_df = validator.validate_rows(raw_df, desk_code, trade_date)
        rejected_count = len(rejected_df)
        logger.info(
            "Validation complete | valid=%d | rejected=%d", len(valid_df), rejected_count
        )

        # LOGIC — write error file if any rows were rejected
        if rejected_count > 0:
            error_key = error_writer.write_error_file(s3_bucket, s3_key, rejected_df)
            logger.info("Error file written | key=%s", error_key)

        # LOGIC — load valid rows to DB
        rows_inserted = loader.load_positions(valid_df)
        logger.info("Rows inserted | rows_inserted=%d", rows_inserted)

        # LOGIC — build and persist summary report
        report_dict = reporter.build_and_store_report(
            s3_bucket,
            s3_key,
            desk_code,
            trade_date,
            raw_df,
            valid_df,
            rejected_df,
            rows_inserted,
        )
        logger.info("Report written | report_keys=%s", list(report_dict.keys()))

        # LOGIC — mark audit as complete with SUCCESS
        audit.complete_audit_record(
            audit_id,
            rows_inserted,
            rejected_count,
            outcome="SUCCESS",
        )

        # LOGIC — send success SNS notification
        notifier.send_success(report_dict)
        logger.info(
            "Pipeline SUCCESS | desk_code=%s | trade_date=%s | audit_id=%s",
            desk_code,
            trade_date,
            audit_id,
        )

        return {
            "status": "SUCCESS",
            "audit_id": audit_id,
            "desk_code": desk_code,
            "trade_date": trade_date,
            "rows_inserted": rows_inserted,
            "rows_rejected": rejected_count,
        }

    except Exception as exc:  # LOGIC — top-level exception handler
        logger.exception(
            "Pipeline FAILURE | desk_code=%s | trade_date=%s | error=%s",
            desk_code,
            trade_date,
            str(exc),
        )

        # LOGIC — attempt to close the audit record as FAILURE
        if audit_id is not None:
            try:
                audit.complete_audit_record(
                    audit_id,
                    rows_inserted,
                    rejected_count,
                    outcome="FAILURE",
                )
            except Exception as audit_exc:
                logger.error("Failed to write FAILURE audit record: %s", str(audit_exc))

        # LOGIC — build failure payload matching SNS failure contract
        error_details = {
            "event_type": "POSITION_INGESTION_FAILURE",
            "desk_code": desk_code,
            "trade_date": trade_date,
            "source_file": s3_key,
            "processing_timestamp": datetime.now(ET).isoformat(),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
        }

        # LOGIC — attempt failure SNS notification (best-effort; do not suppress original exc)
        try:
            notifier.send_failure(error_details)
        except Exception as notify_exc:
            logger.error("Failed to send failure SNS notification: %s", str(notify_exc))

        raise  # LOGIC — re-raise original exception to Lambda runtime