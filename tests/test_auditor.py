import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch, call

import pytz

import auditor


# BOILERPLATE — helper to build a fake DBCredentials-like object
def _make_creds():
    creds = MagicMock()
    creds.host = "db-host"
    creds.port = "5432"
    creds.dbname = "testdb"
    creds.username = "user"
    creds.password = "secret"
    return creds


class TestWriteAuditRecord(unittest.TestCase):

    def _call(self, **overrides):
        """Helper: call write_audit_record with defaults, allowing field overrides."""
        et = pytz.timezone("America/Toronto")
        defaults = dict(
            source_file="EQTY_2026-06-15_positions.csv",
            trade_date="2026-06-15",
            desk_code="EQTY",
            outcome="SUCCESS",
            total_rows=1000,
            rows_loaded=1000,
            rows_rejected=0,
            error_message=None,
            report_key="reports/EQTY_2026-06-15_positions_report.json",
            error_file_key=None,
            processed_at=datetime(2026, 6, 15, 10, 0, 0, tzinfo=et),
            operator_identity="arn:aws:iam::123456789012:role/LambdaRole",
            credentials=_make_creds(),
        )
        defaults.update(overrides)
        auditor.write_audit_record(**defaults)

    @patch("auditor.psycopg2.connect")
    def test_success_executes_upsert(self, mock_connect):
        """LOGIC: write_audit_record calls execute with the correct SQL and commits."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        self._call()

        # LOGIC — verify commit was called (independent transaction)
        mock_conn.commit.assert_called_once()

        # LOGIC — verify execute was called once
        mock_cursor.execute.assert_called_once()
        sql_arg, params_arg = mock_cursor.execute.call_args[0]

        # LOGIC — SQL must reference app.pipeline_audit and ON CONFLICT
        self.assertIn("app.pipeline_audit", sql_arg)
        self.assertIn("ON CONFLICT", sql_arg)
        self.assertIn("DO UPDATE SET", sql_arg)

        # LOGIC — parameters must contain the correct values
        self.assertEqual(params_arg["source_file"], "EQTY_2026-06-15_positions.csv")
        self.assertEqual(params_arg["outcome"], "SUCCESS")
        self.assertEqual(params_arg["total_rows"], 1000)
        self.assertEqual(params_arg["rows_loaded"], 1000)
        self.assertEqual(params_arg["rows_rejected"], 0)
        self.assertIsNone(params_arg["error_message"])
        self.assertIsNone(params_arg["error_file_key"])

    @patch("auditor.psycopg2.connect")
    def test_partial_outcome_params(self, mock_connect):
        """LOGIC: PARTIAL outcome is stored correctly with error_file_key populated."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        self._call(
            outcome="PARTIAL",
            rows_loaded=995,
            rows_rejected=5,
            error_file_key="errors/EQTY_2026-06-15_positions_errors.csv",
        )

        _, params_arg = mock_cursor.execute.call_args[0]
        self.assertEqual(params_arg["outcome"], "PARTIAL")
        self.assertEqual(params_arg["rows_loaded"], 995)
        self.assertEqual(params_arg["rows_rejected"], 5)
        self.assertEqual(
            params_arg["error_file_key"],
            "errors/EQTY_2026-06-15_positions_errors.csv",
        )

    @patch("auditor.psycopg2.connect")
    def test_failure_outcome_params(self, mock_connect):
        """LOGIC: FAILURE outcome stores error_message and nulls for report/error keys."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        self._call(
            outcome="FAILURE",
            total_rows=0,
            rows_loaded=0,
            rows_rejected=0,
            error_message="S3 read failed",
            report_key=None,
            error_file_key=None,
        )

        _, params_arg = mock_cursor.execute.call_args[0]
        self.assertEqual(params_arg["outcome"], "FAILURE")
        self.assertEqual(params_arg["error_message"], "S3 read failed")
        self.assertIsNone(params_arg["report_key"])

    @patch("auditor.psycopg2.connect")
    def test_db_exception_triggers_rollback_and_reraises(self, mock_connect):
        """LOGIC: if psycopg2.execute raises, rollback is called and exception propagates."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("DB error")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        with self.assertRaises(Exception) as ctx:
            self._call()

        self.assertIn("DB error", str(ctx.exception))
        mock_conn.rollback.assert_called_once()

    @patch("auditor.psycopg2.connect")
    def test_connect_called_with_ssl_require(self, mock_connect):
        """LOGIC: psycopg2.connect is called with sslmode='require'."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        creds = _make_creds()
        self._call(credentials=creds)

        mock_connect.assert_called_once_with(
            host=creds.host,
            port=creds.port,
            dbname=creds.dbname,
            user=creds.username,
            password=creds.password,
            sslmode="require",
        )

    @patch("auditor.psycopg2.connect")
    def test_connection_closed_after_success(self, mock_connect):
        """LOGIC: connection is always closed after successful write."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        self._call()

        mock_conn.close.assert_called_once()

    @patch("auditor.psycopg2.connect")
    def test_all_audit_columns_present_in_sql(self, mock_connect):
        """LOGIC: all 12 non-serial columns appear in the INSERT SQL."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        self._call()

        sql_arg, _ = mock_cursor.execute.call_args[0]
        for col in [
            "source_file",
            "trade_date",
            "desk_code",
            "outcome",
            "total_rows",
            "rows_loaded",
            "rows_rejected",
            "error_message",
            "report_key",
            "error_file_key",
            "processed_at",
            "operator_identity",
        ]:
            self.assertIn(col, sql_arg, f"Column '{col}' missing from INSERT SQL")


class TestWriteAuditRecordOperatorIdentity(unittest.TestCase):
    """LOGIC: operator_identity parameter is passed through correctly."""

    @patch("auditor.psycopg2.connect")
    def test_operator_identity_in_params(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        et = pytz.timezone("America/Toronto")
        auditor.write_audit_record(
            source_file="EQTY_2026-06-15_positions.csv",
            trade_date="2026-06-15",
            desk_code="EQTY",
            outcome="SUCCESS",
            total_rows=10,
            rows_loaded=10,
            rows_rejected=0,
            error_message=None,
            report_key="reports/EQTY_2026-06-15_positions_report.json",
            error_file_key=None,
            processed_at=datetime(2026, 6, 15, 9, 0, 0, tzinfo=et),
            operator_identity="arn:aws:iam::123456789012:role/MyRole",
            credentials=_make_creds(),
        )

        _, params_arg = mock_cursor.execute.call_args[0]
        self.assertEqual(
            params_arg["operator_identity"],
            "arn:aws:iam::123456789012:role/MyRole",
        )


if __name__ == "__main__":
    unittest.main()