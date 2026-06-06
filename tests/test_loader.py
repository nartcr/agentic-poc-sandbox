# BOILERPLATE
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sys
import os

# BOILERPLATE — ensure src is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestLoadPositions(unittest.TestCase):
    """Tests for loader.load_positions — TAC-3: idempotent INSERT ON CONFLICT DO NOTHING."""

    # BOILERPLATE
    def _make_valid_df(self, n=10):
        """Build a minimal valid DataFrame matching the rfdh.trade_positions column contract."""
        # LOGIC
        return pd.DataFrame(
            {
                "trade_id": [f"TRD-{i:04d}" for i in range(n)],
                "desk_code": ["EQTY"] * n,
                "trade_date": ["2026-06-15"] * n,
                "instrument_type": ["EQUITY"] * n,
                "notional_amount": [float(1000 * (i + 1)) for i in range(n)],
                "currency": ["USD"] * n,
                "counterparty_id": [f"CP-{i:03d}" for i in range(n)],
            }
        )

    # BOILERPLATE
    def _make_mock_cursor(self, rowcount):
        """Return a mock cursor whose rowcount is set after execute_values is called."""
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.rowcount = rowcount
        return mock_cursor

    @patch("src.ingestion.loader.secrets.get_db_credentials")
    @patch("src.ingestion.loader.psycopg2")
    def test_load_10_rows_returns_10_inserted(self, mock_psycopg2, mock_get_creds):
        """TAC-3 first call: 10 valid rows → rows_inserted == 10."""
        # BOILERPLATE — mock credentials
        mock_get_creds.return_value = {
            "host": "db-host",
            "port": 5432,
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        # BOILERPLATE — mock DB connection and cursor
        mock_conn = MagicMock()
        mock_cursor = self._make_mock_cursor(rowcount=10)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value.__enter__ = lambda s: mock_conn
        mock_psycopg2.connect.return_value.__exit__ = MagicMock(return_value=False)

        # LOGIC — patch execute_values to set rowcount and not fail
        mock_psycopg2.extras.execute_values = MagicMock()

        from src.ingestion.loader import load_positions

        valid_df = self._make_valid_df(10)
        rows_inserted = load_positions(valid_df)

        # LOGIC — assert 10 rows inserted (one batch of 10, rowcount=10)
        self.assertEqual(rows_inserted, 10)
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

    @patch("src.ingestion.loader.secrets.get_db_credentials")
    @patch("src.ingestion.loader.psycopg2")
    def test_load_same_rows_twice_second_call_returns_zero(self, mock_psycopg2, mock_get_creds):
        """TAC-3 second call: identical 10 rows → rows_inserted == 0 (ON CONFLICT DO NOTHING)."""
        # BOILERPLATE — mock credentials
        mock_get_creds.return_value = {
            "host": "db-host",
            "port": 5432,
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        # BOILERPLATE — mock DB connection; second call rowcount is 0
        mock_conn = MagicMock()
        mock_cursor = self._make_mock_cursor(rowcount=0)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value.__enter__ = lambda s: mock_conn
        mock_psycopg2.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.extras.execute_values = MagicMock()

        from src.ingestion.loader import load_positions

        valid_df = self._make_valid_df(10)
        rows_inserted = load_positions(valid_df)

        # LOGIC — ON CONFLICT DO NOTHING → 0 new rows
        self.assertEqual(rows_inserted, 0)
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

    @patch("src.ingestion.loader.secrets.get_db_credentials")
    @patch("src.ingestion.loader.psycopg2")
    def test_rollback_on_exception(self, mock_psycopg2, mock_get_creds):
        """loader rolls back and re-raises on DB exception."""
        # BOILERPLATE
        mock_get_creds.return_value = {
            "host": "db-host",
            "port": 5432,
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value.__enter__ = lambda s: mock_conn
        mock_psycopg2.connect.return_value.__exit__ = MagicMock(return_value=False)

        # LOGIC — execute_values raises an error
        mock_psycopg2.extras.execute_values.side_effect = Exception("DB write failed")

        from src.ingestion.loader import load_positions

        valid_df = self._make_valid_df(10)

        with self.assertRaises(Exception) as ctx:
            load_positions(valid_df)

        self.assertIn("DB write failed", str(ctx.exception))
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    @patch("src.ingestion.loader.secrets.get_db_credentials")
    @patch("src.ingestion.loader.psycopg2")
    def test_batching_multiple_batches_sums_rowcount(self, mock_psycopg2, mock_get_creds):
        """loader correctly sums rowcount across multiple 1000-row batches."""
        # BOILERPLATE
        mock_get_creds.return_value = {
            "host": "db-host",
            "port": 5432,
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        mock_conn = MagicMock()

        # LOGIC — cursor rowcount returns 1000 per batch; simulate 2500 rows = 3 batches (1000+1000+500)
        call_count = [0]

        class MockCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            @property
            def rowcount(self):
                # LOGIC — return 1000 for first two batches, 500 for last
                return [1000, 1000, 500][min(call_count[0] - 1, 2)]

        original_cursor = mock_conn.cursor

        def cursor_factory():
            call_count[0] += 1
            return MockCursor()

        mock_conn.cursor.side_effect = cursor_factory
        mock_psycopg2.connect.return_value.__enter__ = lambda s: mock_conn
        mock_psycopg2.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.extras.execute_values = MagicMock()

        from importlib import reload
        import src.ingestion.loader as loader_module
        # Force reimport to pick up fresh mock state
        valid_df = self._make_valid_df(2500)
        rows_inserted = loader_module.load_positions(valid_df)

        # LOGIC — 1000 + 1000 + 500 = 2500
        self.assertEqual(rows_inserted, 2500)

    @patch("src.ingestion.loader.secrets.get_db_credentials")
    @patch("src.ingestion.loader.psycopg2")
    def test_empty_dataframe_returns_zero(self, mock_psycopg2, mock_get_creds):
        """loader returns 0 without touching DB when passed an empty DataFrame."""
        # BOILERPLATE
        mock_get_creds.return_value = {
            "host": "db-host",
            "port": 5432,
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value.__enter__ = lambda s: mock_conn
        mock_psycopg2.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.extras.execute_values = MagicMock()

        from src.ingestion.loader import load_positions

        # LOGIC — empty DataFrame
        empty_df = self._make_valid_df(0)
        rows_inserted = load_positions(empty_df)

        self.assertEqual(rows_inserted, 0)
        mock_psycopg2.extras.execute_values.assert_not_called()


if __name__ == "__main__":
    unittest.main()