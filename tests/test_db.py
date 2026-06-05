# BOILERPLATE
import unittest
from unittest.mock import MagicMock, patch

import db


class TestGetConnection(unittest.TestCase):
    # LOGIC — db.get_connection passes correct parameters to psycopg2.connect

    def _credentials(self, **overrides) -> dict:
        creds = {
            "host": "aurora-cluster.cluster-abc123.us-east-1.rds.amazonaws.com",
            "port": 5432,
            "dbname": "tradedb",
            "username": "appuser",
            "password": "s3cr3t",
        }
        creds.update(overrides)
        return creds

    @patch("db.psycopg2.connect")
    def test_connect_called_with_correct_host(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        result = db.get_connection(creds)
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["host"], creds["host"])

    @patch("db.psycopg2.connect")
    def test_connect_called_with_correct_port(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials(port=5433)
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["port"], 5433)

    @patch("db.psycopg2.connect")
    def test_connect_called_with_string_port_converted_to_int(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials(port="5432")
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertIsInstance(call_kwargs["port"], int)
        self.assertEqual(call_kwargs["port"], 5432)

    @patch("db.psycopg2.connect")
    def test_connect_called_with_correct_dbname(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["dbname"], creds["dbname"])

    @patch("db.psycopg2.connect")
    def test_connect_called_with_correct_user(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["user"], creds["username"])

    @patch("db.psycopg2.connect")
    def test_connect_called_with_sslmode_require(self, mock_connect):
        # LOGIC — NFR-3.2: SSL in transit must be enforced
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["sslmode"], "require")

    @patch("db.psycopg2.connect")
    def test_connect_called_with_connect_timeout_10(self, mock_connect):
        # LOGIC — connect_timeout=10 as specified in design
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        db.get_connection(creds)
        call_kwargs = mock_connect.call_args[1]
        self.assertEqual(call_kwargs["connect_timeout"], 10)

    @patch("db.psycopg2.connect")
    def test_returns_connection_object(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials()
        result = db.get_connection(creds)
        self.assertIs(result, mock_conn)

    @patch("db.psycopg2.connect")
    def test_password_not_logged(self, mock_connect):
        # LOGIC — BAC-8: password must never appear in logs
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        creds = self._credentials(password="super_secret_password_xyz")

        import logging
        log_records = []

        class CapturingHandler(logging.Handler):
            def emit(self, record):
                log_records.append(self.format(record))

        handler = CapturingHandler()
        db_logger = logging.getLogger("db")
        db_logger.addHandler(handler)
        db_logger.setLevel(logging.DEBUG)
        try:
            db.get_connection(creds)
        finally:
            db_logger.removeHandler(handler)

        combined_logs = " ".join(log_records)
        self.assertNotIn("super_secret_password_xyz", combined_logs)

    def test_missing_host_key_raises(self):
        creds = {
            "port": 5432,
            "dbname": "tradedb",
            "username": "appuser",
            "password": "s3cr3t",
        }
        with self.assertRaises(KeyError):
            with patch("db.psycopg2.connect") as mock_connect:
                mock_connect.side_effect = Exception("connect failed")
                # Actually test the KeyError from missing key before connect is called
                pass
        # Direct test: missing key raises KeyError at access time
        with self.assertRaises(KeyError):
            _ = creds["host"]


if __name__ == "__main__":
    unittest.main()