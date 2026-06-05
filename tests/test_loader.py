# BOILERPLATE
import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock, patch, call

import pandas as pd


class TestLoader(unittest.TestCase):

    def setUp(self):
        if "config" in sys.modules:
            del sys.modules["config"]
        env = {
            "S3_BUCKET": "b", "S3_INPUT_PREFIX": "i/", "S3_REPORTS_PREFIX": "r/",
            "S3_ERRORS_PREFIX": "e/", "DB_SECRET_ID": "sid",
            "SNS_TOPIC_ARN_SUCCESS": "arn:s", "SNS_TOPIC_ARN_FAILURE": "arn:f",
            "AWS_REGION": "us-east-1",
        }
        self._env_patch = patch.dict(os.environ, env, clear=True)
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()
        for mod in ["config", "loader", "exceptions"]:
            if mod in sys.modules:
                del sys.modules[mod]

    def _make_valid_df(self):
        return pd.DataFrame([{
            "trade_id": "T001",
            "desk_code": "EQTY",
            "trade_date": date(2024, 1, 15),
            "instrument_type": "SWAP",
            "notional_amount": 1000000.0,
            "currency": "USD",
            "counterparty_id": "CP01",
        }])

    def _make_credentials(self):
        return {
            "host": "localhost",
            "port": 5432,
            "dbname": "trades",
            "username": "user",
            "password": "pass",
        }

    # LOGIC
    def test_empty_df_returns_zero_without_db_call(self):
        with patch("psycopg2.connect") as mock_connect:
            import loader
            result = loader.load_trades(pd.DataFrame(), "file.csv", self._make_credentials())
        self.assertEqual(result, 0)
        mock_connect.assert_not_called()

    def test_successful_insert_returns_rowcount(self):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values") as mock_ev:
                import loader
                result = loader.load_trades(self._make_valid_df(), "file.csv", self._make_credentials())
        self.assertEqual(result, 1)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_raises_load_error_on_db_exception(self):
        with patch("psycopg2.connect", side_effect=Exception("connection refused")):
            import loader
            from exceptions import LoadError
            with self.assertRaises(LoadError) as ctx:
                loader.load_trades(self._make_valid_df(), "file.csv", self._make_credentials())
        self.assertIn("connection refused", str(ctx.exception))

    def test_rollback_called_on_exception(self):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=Exception("insert failed")):
                import loader
                from exceptions import LoadError
                with self.assertRaises(LoadError):
                    loader.load_trades(self._make_valid_df(), "file.csv", self._make_credentials())
        mock_conn.rollback.assert_called_once()

    def test_connection_closed_in_finally(self):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values"):
                import loader
                loader.load_trades(self._make_valid_df(), "file.csv", self._make_credentials())
        mock_conn.close.assert_called_once()

    def test_source_file_and_loaded_at_added_to_rows(self):
        """Verify execute_values receives rows with source_file included."""
        mock_cursor = MagicMock()
        mock_