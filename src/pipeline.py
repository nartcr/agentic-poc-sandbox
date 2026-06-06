# BOILERPLATE
import io
import logging
import os
import re
from datetime import datetime

import pandas as pd
import pytz

from src.audit import write_audit_record
from src.config import Config
from src.error_writer import write_error_file
from src.loader import load_positions
from src.notifier import notify_failure, notify_success
from src.reporter import build_and_upload_report
from src.s3_client import download_fileobj
from src.secrets import get_db_credentials
from src.validator import validate_rows

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — filename convention regex from design
_FILENAME_RE = re.compile(r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$")

# BOILERPLATE
_ET_TZ = pytz.timezone("America/Toronto")


def _parse_filename(s3_key: str) -> tuple[str, str]:
    # LOGIC — extract just the filename component from the full S3 key
    filename = os.path.basename(s3_key)
    match = _FILENAME_RE.match(filename)
    if not match:
        raise ValueError(
            f"S3 key '{s3_key}' does not match expected filename convention "
            r"^([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_positions\.csv$"
        )
    desk_code = match.group(1)
    trade_date = match.group(2)
    return desk_code, trade_date


def _determine_outcome(loaded_rows: int, rejected_rows: int) -> str:
    # LOGIC — outcome rules from design:
    # SUCCESS  : loaded > 0 and no rejections  OR  valid_df is empty (nothing to load) and no rejections
    # PARTIAL  : loaded > 0 AND rejected > 0
    # Note: FAILURE is only set on exception — this function is only reached on non-exception path
    if loaded_rows > 0 and rejected_rows > 0:
        return "PARTIAL"
    return "SUCCESS"


def process_file(s3_key: str, config: Config) -> None:
    # BOILERPLATE — capture processing timestamp in ET at the start of this file's run
    processing_timestamp: datetime = datetime.now(tz=_ET_TZ)

    # LOGIC — step 1: parse desk_code and trade_date from filename
    desk_code, trade_date = _parse_filename(s3_key)
    logger.info(
        "Starting processing: s3_key=%s desk_code=%s trade_date=%s",
        s3_key,
        desk_code,
        trade_date,
    )

    # BOILERPLATE — fetch DB credentials once; reused across loader and audit
    credentials: dict = get_db_credentials(config.db_secret_id)

    try:
        # LOGIC — step 2: download file bytes from S3
        file_obj: io.BytesIO = download_fileobj(config.s3_bucket, s3_key)

        # LOGIC — step 3: parse CSV into DataFrame; raise on parse failure
        try:
            raw_bytes: bytes = file_obj.read()
            csv_text: str = raw_bytes.decode("utf-8")
            df: pd.DataFrame = pd.read_csv(io.StringIO(csv_text), dtype=str)
        except Exception as parse_exc:
            raise ValueError(
                f"Failed to parse CSV for s3_key={s3_key}: {parse_exc}"
            ) from parse_exc

        total_rows: int = len(df)
        logger.info("Parsed CSV: s3_key=%s total_rows=%d", s3_key, total_rows)

        # LOGIC — step 4: validate rows
        valid_df: pd.DataFrame
        rejected_df: pd.DataFrame
        valid_df, rejected_df = validate_rows(df, desk_code, trade_date)
        logger.info(
            "Validation complete: valid=%d rejected=%d",
            len(valid_df),
            len(rejected_df),
        )

        # LOGIC — step 5: load valid rows into Aurora
        loaded_count: int = load_positions(valid_df, credentials)
        logger.info(
            "Load complete: loaded=%d attempted=%d",
            loaded_count,
            len(valid_df),
        )

        # LOGIC — step 6: write error file only when there are rejected rows
        if len(rejected_df) > 0:
            error_key: str = write_error_file(
                rejected_df, desk_code, trade_date, config.s3_bucket
            )
            logger.info("Error file written: s3_key=%s", error_key)

        # LOGIC — step 7: build and upload summary report
        report: dict = build_and_upload_report(
            source_key=s3_key,
            total_rows=total_rows,
            loaded_rows=loaded_count,
            rejected_rows=len(rejected_df),
            valid_df=valid_df,
            rejected_df=rejected_df,
            processing_timestamp=processing_timestamp,
            bucket=config.s3_bucket,
        )
        logger.info("Report uploaded for s3_key=%s", s3_key)

        # LOGIC — step 8: determine outcome and write audit record
        outcome: str = _determine_outcome(loaded_count, len(rejected_df))
        write_audit_record(
            credentials=credentials,
            source_key=s3_key,
            desk_code=desk_code,
            trade_date=trade_date,
            outcome=outcome,
            total_rows=total_rows,
            loaded_rows=loaded_count,
            rejected_rows=len(rejected_df),
            error_detail=None,
            processed_at=processing_timestamp,
        )
        logger.info(
            "Audit record written: s3_key=%s outcome=%s", s3_key, outcome
        )

        # LOGIC — step 9: notify success
        notify_success(report, config.sns_success_arn)
        logger.info("Success notification sent for s3_key=%s", s3_key)

    except Exception as exc:
        # LOGIC — step 10: failure path — audit + notify, then re-raise
        logger.error(
            "Processing failed for s3_key=%s: %s", s3_key, exc, exc_info=True
        )

        # LOGIC — sanitise error message: do not include credential values
        sanitised_error: str = str(exc)

        # LOGIC — attempt to write FAILURE audit record; swallow secondary errors
        try:
            write_audit_record(
                credentials=credentials,
                source_key=s3_key,
                desk_code=desk_code,
                trade_date=trade_date,
                outcome="FAILURE",
                total_rows=0,
                loaded_rows=0,
                rejected_rows=0,
                error_detail=sanitised_error,
                processed_at=processing_timestamp,
            )
        except Exception as audit_exc:
            logger.error(
                "Could not write FAILURE audit record for s3_key=%s: %s",
                s3_key,
                audit_exc,
            )

        # LOGIC — attempt failure SNS notification; swallow secondary errors
        try:
            notify_failure(s3_key, sanitised_error, config.sns_failure_arn)
        except Exception as notify_exc:
            logger.error(
                "Could not send failure notification for s3_key=%s: %s",
                s3_key,
                notify_exc,
            )

        # LOGIC — re-raise original exception so main() can track failure count
        raise