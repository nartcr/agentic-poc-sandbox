import json
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytz


# BOILERPLATE — patch all sibling modules before importing handler
# This prevents import-time side effects and isolates the handler logic


def _make_raw_df():
    """BOILERPLATE — minimal valid raw DataFrame as returned by file_reader."""
    return pd.DataFrame(
        {
            "trade_id": ["T001", "T002", "T003"],
            "desk_code": ["EQTY", "EQTY", "EQTY"],
            "trade_date": ["2026-06-15", "2026-06-15", "2026-06-15"],
            "instrument_type": ["EQ", "EQ", "EQ"],
            "notional_amount": ["1000.00", "2000.00", "3000.00"],
            "currency": ["USD", "USD", "USD"],
            "counterparty_id": ["CP1", "CP2", "CP3"],
            "_source_row": [1, 2, 3],
        }
    )


def _make_valid_df():
    """BOILERPLATE — valid DataFrame with proper types as returned by validator."""
    df = _make_raw_df().copy()
    df["notional_amount"] = df["notional_amount"].astype(float)
    return df


def _make_empty_rejected_df():
    """BOILERPLATE — empty rejected DataFrame (all rows valid)."""
    return pd.DataFrame(
        columns=[
            "trade_id",
            "desk_code",
            "trade_date",
            "instrument_type",
            "notional_amount",
            "currency",
            "counterparty_id",
            "_source_row",
            "rejection_reason",
        ]
    )


def _make_s3_event(bucket="test-bucket", key="positions/EQTY_2026-06-15_positions.csv"):
    """BOILERPLATE — minimal S3 Lambda trigger event."""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


def _make_config():
    """BOILERPLATE — fake Config object."""
    cfg = MagicMock()
    cfg.S3_BUCKET = "test-bucket"
    cfg.S3_INPUT_PREFIX = "positions/"
    cfg.S3_REPORTS_PREFIX = "reports/"
    cfg.S3_ERRORS_PREFIX = "errors/"
    cfg.DB_SECRET_ID = "prod/db/secret"
    cfg.SNS_SUCCESS_TOPIC_ARN = "arn:aws:sns:ca-central-1:123456789012:success"
    cfg.SNS_FAILURE_TOPIC_ARN = "arn:aws:sns:ca-central-1:123456789012:failure"
    cfg.AUDIT_TABLE = "app.pipeline_audit"
    cfg.TZ = "America/Toronto"
    return cfg


def _make_report():
    """BOILERPLATE — minimal report dict."""
    return {
        "source_file": "EQTY_2026-06-15_positions.csv",
        "trade_date": "2026-06-15",
        "desk_code": "EQTY",
        "load_timestamp": "2026-06-15T10:00:00-04:00",
        "total_rows_received": 3,
        "rows_loaded": 3,
        "rows_rejected": 0,
        "rows_skipped_duplicate": 0,
        "desk_code_counts": {"EQTY": 3},
        "notional_amount_min": 1000.0,
        "notional_amount_max": 3000.0,
        "null_rates": {
            "trade_id": 0.0,
            "desk_code": 0.0,
            "trade_date": 0.0,
            "instrument_type": 0.0,
            "notional_amount": 0.0,
            "currency": 0.0,
            "counterparty_id": 0.0,
        },
        "error_file_key": None,
    }


class TestLambdaHandlerSuccess(unittest.TestCase):
    """LOGIC: happy path — all rows valid, zero rejections, SUCCESS outcome."""

    def setUp(self):
        # BOILERPLATE — set required environment variables before importing handler
        os.environ["S3_BUCKET"] = "test-bucket"
        os.environ["S3_INPUT_PREFIX"] = "positions/"
        os.environ["S3_REPORTS_PREFIX"] = "reports/"
        os.environ["S3_ERRORS_PREFIX"] = "errors/"
        os.environ["DB_SECRET_ID"] = "prod/db/secret"
        os.environ["SNS_SUCCESS_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:success"
        os.environ["SNS_FAILURE_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:failure"
        os.environ["AUDIT_TABLE"] = "app.pipeline_audit"
        os.environ["TZ"] = "America/Toronto"
        os.environ["OPERATOR_IDENTITY"] = "arn:aws:iam::123456789012:role/TestRole"

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_happy_path_returns_200(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: full happy path returns statusCode 200 with report body."""
        import handler

        # BOILERPLATE — wire up mock return values
        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df()
        rejected_df = _make_empty_rejected_df()
        source_file = "EQTY_2026-06-15_positions.csv"

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, source_file)
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 3
        mock_reporter.build_report.return_value = _make_report()
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        event = _make_s3_event()
        result = handler.lambda_handler(event, None)

        # LOGIC — verify response shape
        self.assertEqual(result["statusCode"], 200)
        body = json.loads(result["body"])
        self.assertEqual(body["source_file"], "EQTY_2026-06-15_positions.csv")

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_no_error_file_when_zero_rejections(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: error_writer is NOT called when rejected_df is empty."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df()
        rejected_df = _make_empty_rejected_df()

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 3
        mock_reporter.build_report.return_value = _make_report()
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        event = _make_s3_event()
        handler.lambda_handler(event, None)

        # LOGIC — error_writer.write_error_file must NOT be called
        mock_error_writer.write_error_file.assert_not_called()

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_success_outcome_written_to_audit(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: audit record outcome is 'SUCCESS' when zero rows rejected."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df()
        rejected_df = _make_empty_rejected_df()

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 3
        mock_reporter.build_report.return_value = _make_report()
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        event = _make_s3_event()
        handler.lambda_handler(event, None)

        # LOGIC — audit must be called with outcome="SUCCESS"
        mock_auditor.write_audit_record.assert_called_once()
        call_kwargs = mock_auditor.write_audit_record.call_args[1]
        self.assertEqual(call_kwargs["outcome"], "SUCCESS")
        self.assertEqual(call_kwargs["rows_loaded"], 3)
        self.assertEqual(call_kwargs["rows_rejected"], 0)

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_success_sns_published(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: notifier.publish_success is called exactly once with the report."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df()
        rejected_df = _make_empty_rejected_df()
        report = _make_report()

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 3
        mock_reporter.build_report.return_value = report
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        event = _make_s3_event()
        handler.lambda_handler(event, None)

        mock_notifier.publish_success.assert_called_once_with(
            report,
            _make_config().SNS_SUCCESS_TOPIC_ARN,
        )


class TestLambdaHandlerPartial(unittest.TestCase):
    """LOGIC: partial success — some rows rejected, outcome = PARTIAL."""

    def setUp(self):
        os.environ["S3_BUCKET"] = "test-bucket"
        os.environ["S3_INPUT_PREFIX"] = "positions/"
        os.environ["S3_REPORTS_PREFIX"] = "reports/"
        os.environ["S3_ERRORS_PREFIX"] = "errors/"
        os.environ["DB_SECRET_ID"] = "prod/db/secret"
        os.environ["SNS_SUCCESS_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:success"
        os.environ["SNS_FAILURE_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:failure"
        os.environ["AUDIT_TABLE"] = "app.pipeline_audit"
        os.environ["TZ"] = "America/Toronto"
        os.environ["OPERATOR_IDENTITY"] = "lambda"

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_partial_outcome_and_error_file_written(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: when rejected_df is non-empty, error file is written and outcome=PARTIAL."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df().iloc[:2]  # only 2 valid rows

        # LOGIC — build a rejected_df with 1 row
        rejected_df = pd.DataFrame(
            {
                "trade_id": ["T003"],
                "desk_code": ["EQTY"],
                "trade_date": ["bad-date"],
                "instrument_type": ["EQ"],
                "notional_amount": ["3000.00"],
                "currency": ["USD"],
                "counterparty_id": ["CP3"],
                "_source_row": [3],
                "rejection_reason": ["trade_date is not a valid date (expected YYYY-MM-DD)"],
            }
        )

        report = _make_report()
        report["rows_rejected"] = 1
        report["rows_loaded"] = 2

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 2
        mock_error_writer.write_error_file.return_value = (
            "errors/EQTY_2026-06-15_positions_errors.csv"
        )
        mock_reporter.build_report.return_value = report
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        event = _make_s3_event()
        result = handler.lambda_handler(event, None)

        # LOGIC — error file must be written
        mock_error_writer.write_error_file.assert_called_once()

        # LOGIC — audit outcome must be PARTIAL
        call_kwargs = mock_auditor.write_audit_record.call_args[1]
        self.assertEqual(call_kwargs["outcome"], "PARTIAL")
        self.assertEqual(call_kwargs["rows_rejected"], 1)
        self.assertEqual(call_kwargs["rows_loaded"], 2)

        # LOGIC — still returns 200
        self.assertEqual(result["statusCode"], 200)


class TestLambdaHandlerFailure(unittest.TestCase):
    """LOGIC: exception path — failure SNS published, FAILURE audit written, exception re-raised."""

    def setUp(self):
        os.environ["S3_BUCKET"] = "test-bucket"
        os.environ["S3_INPUT_PREFIX"] = "positions/"
        os.environ["S3_REPORTS_PREFIX"] = "reports/"
        os.environ["S3_ERRORS_PREFIX"] = "errors/"
        os.environ["DB_SECRET_ID"] = "prod/db/secret"
        os.environ["SNS_SUCCESS_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:success"
        os.environ["SNS_FAILURE_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:failure"
        os.environ["AUDIT_TABLE"] = "app.pipeline_audit"
        os.environ["TZ"] = "America/Toronto"
        os.environ.pop("OPERATOR_IDENTITY", None)

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_file_reader_exception_triggers_failure_path(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: if file_reader raises, publish_failure is called and exception re-raises."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()
        mock_file_reader.read_csv_from_s3.side_effect = RuntimeError("S3 read failed")

        event = _make_s3_event()

        with self.assertRaises(RuntimeError) as ctx:
            handler.lambda_handler(event, None)

        # LOGIC — failure SNS must be published
        mock_notifier.publish_failure.assert_called_once()
        call_kwargs = mock_notifier.publish_failure.call_args[1]
        self.assertIn("S3 read failed", call_kwargs["error_message"])
        self.assertEqual(call_kwargs["topic_arn"], _make_config().SNS_FAILURE_TOPIC_ARN)

        # LOGIC — exception is re-raised
        self.assertIn("S3 read failed", str(ctx.exception))

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_failure_audit_written_when_credentials_available(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: FAILURE audit record written if credentials were fetched before exception."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        creds = MagicMock()
        mock_secrets.get_db_credentials.return_value = creds

        # LOGIC — validator raises after credentials are set
        raw_df = _make_raw_df()
        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.side_effect = ValueError("Validation exploded")

        event = _make_s3_event()

        with self.assertRaises(ValueError):
            handler.lambda_handler(event, None)

        # LOGIC — audit write_audit_record must be called with outcome=FAILURE
        mock_auditor.write_audit_record.assert_called_once()
        call_kwargs = mock_auditor.write_audit_record.call_args[1]
        self.assertEqual(call_kwargs["outcome"], "FAILURE")
        self.assertEqual(call_kwargs["error_message"], "Validation exploded")
        self.assertIs(call_kwargs["credentials"], creds)

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_no_audit_when_credentials_unavailable(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: audit write is skipped (not called) if credentials fetch itself failed."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        # LOGIC — secrets fetch fails before credentials are assigned
        mock_secrets.get_db_credentials.side_effect = RuntimeError("Secrets Manager unavailable")

        event = _make_s3_event()

        with self.assertRaises(RuntimeError):
            handler.lambda_handler(event, None)

        # LOGIC — auditor must NOT be called (credentials is None)
        mock_auditor.write_audit_record.assert_not_called()

        # LOGIC — but failure SNS must still be published
        mock_notifier.publish_failure.assert_called_once()

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_sns_failure_exception_does_not_suppress_original_error(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: if SNS publish_failure itself raises, the original exception is still re-raised."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()
        mock_file_reader.read_csv_from_s3.side_effect = RuntimeError("original error")
        mock_notifier.publish_failure.side_effect = RuntimeError("SNS also failed")

        event = _make_s3_event()

        # LOGIC — original RuntimeError must propagate (not the SNS error)
        with self.assertRaises(RuntimeError) as ctx:
            handler.lambda_handler(event, None)

        self.assertIn("original error", str(ctx.exception))


class TestLambdaHandlerS3KeyParsing(unittest.TestCase):
    """LOGIC: S3 key URL-decoding and source_file extraction."""

    def setUp(self):
        os.environ["S3_BUCKET"] = "test-bucket"
        os.environ["S3_INPUT_PREFIX"] = "positions/"
        os.environ["S3_REPORTS_PREFIX"] = "reports/"
        os.environ["S3_ERRORS_PREFIX"] = "errors/"
        os.environ["DB_SECRET_ID"] = "prod/db/secret"
        os.environ["SNS_SUCCESS_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:success"
        os.environ["SNS_FAILURE_TOPIC_ARN"] = "arn:aws:sns:ca-central-1:123456789012:failure"
        os.environ["AUDIT_TABLE"] = "app.pipeline_audit"
        os.environ["TZ"] = "America/Toronto"

    @patch("handler.auditor")
    @patch("handler.notifier")
    @patch("handler.reporter")
    @patch("handler.error_writer")
    @patch("handler.loader")
    @patch("handler.validator")
    @patch("handler.file_reader")
    @patch("handler.secrets_module")
    @patch("handler.config_module")
    def test_url_encoded_key_is_decoded(
        self,
        mock_config_module,
        mock_secrets,
        mock_file_reader,
        mock_validator,
        mock_loader,
        mock_error_writer,
        mock_reporter,
        mock_notifier,
        mock_auditor,
    ):
        """LOGIC: URL-encoded S3 key (space as +) is decoded before passing to file_reader."""
        import handler

        mock_config_module.Config.return_value = _make_config()
        mock_secrets.get_db_credentials.return_value = MagicMock()

        raw_df = _make_raw_df()
        valid_df = _make_valid_df()
        rejected_df = _make_empty_rejected_df()

        mock_file_reader.read_csv_from_s3.return_value = (raw_df, "EQTY_2026-06-15_positions.csv")
        mock_validator.validate_rows.return_value = (valid_df, rejected_df)
        mock_loader.load_trades.return_value = 3
        mock_reporter.build_report.return_value = _make_report()
        mock_reporter.write_report.return_value = "reports/EQTY_2026-06-15_positions_report.json"

        # LOGIC — URL-encoded key with space encoded as +
        event = _make_s3_event(key="positions/EQTY_2026-06-15_positions.csv")
        handler.lambda_handler(event, None)

        # LOGIC — file_reader must receive the decoded key
        call_args = mock_file_reader.read_csv_from_s3.call_args
        self.assertEqual(call_args[0][1], "positions/EQTY_2026-06-15_positions.csv")


if __name__ == "__main__":
    unittest.main()