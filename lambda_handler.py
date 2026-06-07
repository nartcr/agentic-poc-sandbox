# BOILERPLATE
import logging
import re
from datetime import datetime
from urllib.parse import unquote_plus

import pytz

from audit_writer import write_audit_record
from db_loader import load_positions
from error_writer import write_error_file
from pipeline_config import PipelineConfig
from position_validator import validate_positions
from report_builder import build_report, write_report_to_s3
from s3_reader import read_position_file
from secret_manager import get_db_credentials
from sns_notifier import notify_failure, notify_success

# BOILERPLATE — module-level logger; Lambda configures the root handler
logger = logging.getLogger(__name__)

# LOGIC — filename pattern: incoming/{desk_code}_{trade_date}_positions.csv
_FILENAME_PATTERN = re.compile(
    r"^(?:.+/)?(?P<desk_code>[^/]+?)_(?P<trade_date>\d{4}-\d{2}-\d{2})_positions\.csv$"
)

_ET_TZ = pytz.timezone("America/Toronto")


def _parse_filename(key: str) -> tuple[str, str]:
    """Extract desk_code and trade_date from the S3 object key."""  # LOGIC
    match = _FILENAME_PATTERN.match(key)
    if not match:
        raise ValueError(
            f"S3 key '{key}' does not match expected pattern "
            "'{{desk_code}}_{{trade_date}}_positions.csv'"
        )
    return match.group("desk_code"), match.group("trade_date")


def _determine_status(rows_rejected: int, rows_loaded: int, valid_count: int) -> str:
    """Determine pipeline audit status: SUCCESS, PARTIAL."""  # LOGIC
    if rows_rejected > 0 and (rows_loaded > 0 or valid_count > rows_loaded):
        return "PARTIAL"
    if rows_rejected > 0 and rows_loaded == 0:
        return "PARTIAL"
    return "SUCCESS"


def handler(event: dict, context: object) -> None:  # LOGIC
    """AWS Lambda entry point — orchestrates the full trade positions pipeline."""

    # BOILERPLATE — capture ET timestamp at invocation start; used by all downstream writes
    processed_at_et: datetime = datetime.now(tz=_ET_TZ)

    # BOILERPLATE — initialise state variables used in the failure handler
    config: PipelineConfig | None = None
    file_key: str | None = None
    desk_code: str | None = None
    trade_date: str | None = None
    error_s3_key: str | None = None
    report_s3_key: str | None = None
    total_rows: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0

    try:
        # LOGIC — Step 1: load configuration from environment variables
        config = PipelineConfig()
        logger.info("Pipeline configuration loaded successfully.")

        # LOGIC — Step 2: extract bucket and key from the S3 ObjectCreated event
        record = event["Records"][0]
        bucket: str = record["s3"]["bucket"]["name"]
        raw_key: str = record["s3"]["object"]["key"]
        file_key = unquote_plus(raw_key)  # handle URL-encoding from S3 event

        logger.info("Processing S3 event: bucket=%s key=%s", bucket, file_key)

        # LOGIC — parse desk_code and trade_date from filename
        desk_code, trade_date = _parse_filename(file_key)
        logger.info(
            "Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date
        )

        # LOGIC — Step 3: retrieve Aurora credentials from Secrets Manager at runtime
        credentials = get_db_credentials(config.db_secret_id)

        # LOGIC — Step 4: read the CSV from S3 into a DataFrame (all columns as str)
        raw_df, s3_key = read_position_file(bucket, file_key)
        total_rows = len(raw_df)
        logger.info(
            "File read: key=%s total_rows=%d", s3_key, total_rows
        )

        # LOGIC — Step 5: validate rows; split into valid and rejected DataFrames
        valid_df, rejected_df = validate_positions(raw_df)
        rows_rejected = len(rejected_df)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            rows_rejected,
        )

        # LOGIC — Step 6: write error file to S3 if there are any rejected rows
        error_s3_key = write_error_file(
            rejected_df=rejected_df,
            bucket=bucket,
            desk_code=desk_code,
            trade_date=trade_date,
            processed_at_et=processed_at_et,
        )
        if error_s3_key:
            logger.info("Error file written: %s", error_s3_key)

        # LOGIC — Step 7: load valid rows into Aurora via ON CONFLICT DO NOTHING
        rows_loaded = load_positions(
            valid_df=valid_df,
            credentials=credentials,
        )
        logger.info(
            "DB load complete: rows_loaded=%d rows_skipped=%d",
            rows_loaded,
            len(valid_df) - rows_loaded,
        )

        # LOGIC — Step 8: build the summary report dict and write JSON to S3
        report_dict = build_report(
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_loaded=rows_loaded,
            processed_at_et=processed_at_et,
            desk_code=desk_code,
            trade_date=trade_date,
            source_file_key=file_key,
        )
        report_s3_key = write_report_to_s3(
            report_dict=report_dict,
            bucket=bucket,
            processed_at_et=processed_at_et,
        )
        logger.info("Report written: %s", report_s3_key)

        # LOGIC — Step 9: write audit record to demo_schema.pipeline_audit (upsert)
        status = _determine_status(rows_rejected, rows_loaded, len(valid_df))
        write_audit_record(
            credentials=credentials,
            file_key=file_key,
            desk_code=desk_code,
            trade_date=trade_date,
            status=status,
            total_rows=total_rows,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            error_message=None,
            processed_at_et=processed_at_et,
            report_s3_key=report_s3_key,
            error_s3_key=error_s3_key,
        )
        logger.info("Audit record written: status=%s", status)

        # LOGIC — Step 10: publish success notification to SNS
        # Attach report_s3_key to report_dict so notify_success can include it
        report_dict["report_s3_key"] = report_s3_key
        notify_success(config=config, report_dict=report_dict)
        logger.info(
            "Pipeline completed successfully: file_key=%s status=%s", file_key, status
        )

    except Exception as exc:  # LOGIC — catch-all for unhandled exceptions
        error_message = str(exc)
        logger.exception(
            "Pipeline failed for file_key=%s: %s", file_key, error_message
        )

        # LOGIC — attempt audit write even on failure; best-effort with available values
        if config is not None and file_key is not None:
            try:
                credentials = get_db_credentials(config.db_secret_id)
                write_audit_record(
                    credentials=credentials,
                    file_key=file_key,
                    desk_code=desk_code or "UNKNOWN",
                    trade_date=trade_date or "1970-01-01",
                    status="FAILURE",
                    total_rows=total_rows,
                    rows_loaded=rows_loaded,
                    rows_rejected=rows_rejected,
                    error_message=error_message,
                    processed_at_et=processed_at_et,
                    report_s3_key=report_s3_key,
                    error_s3_key=error_s3_key,
                )
                logger.info("Failure audit record written for file_key=%s", file_key)
            except Exception as audit_exc:
                # LOGIC — log but do not suppress; re-raise original exception below
                logger.error(
                    "Failed to write failure audit record: %s", str(audit_exc)
                )

        # LOGIC — attempt failure SNS notification; best-effort
        if config is not None:
            try:
                failure_details = {
                    "source_file_key": file_key,
                    "desk_code": desk_code,
                    "trade_date": trade_date,
                    "processed_at_et": processed_at_et.isoformat(),
                    "error_message": error_message,
                    "error_s3_key": error_s3_key,
                }
                notify_failure(config=config, error_details=failure_details)
            except Exception as sns_exc:
                logger.error(
                    "Failed to publish failure SNS notification: %s", str(sns_exc)
                )

        # LOGIC — re-raise so Lambda marks invocation as failed (enables DLQ / retry)
        raise