# BOILERPLATE
import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pandas as pd

import loader


def _make_credentials():
    # BOILERPLATE — minimal DBCredentials stand-in
    return SimpleNamespace(
        host="db.example.internal",
        port="5432",
        dbname="trading",
        username="svc_user",
        password="REDACTED",
    )


def _make_valid_df(n=3):
    # LOGIC — build a minimal valid DataFrame matching the post-validation schema
    rows = [
        {
            "trade_id": f"T{i:04d}",
            "desk_code": "EQTY",
            "trade_date": date(2026, 6, 15),
            "instrument_type": "EQUITY",
            "notional_amount": float(1000 * i),
            "currency": "USD",
            "counterparty_id": f"CP{i:04d}",
            "_source_row": i,
        }
        for i in range(1, n + 1)
    ]
    return pd.DataFrame(rows)


class TestLoadTradesEmpty(unittest.TestCase):
    def test_empty_dataframe_returns_zero(self):
        # LOGIC — empty df must short-circuit and return 0 without touching DB
        empty_df = pd.DataFrame(
            columns=[
                "trade_id", "desk_code", "trade_date", "instrument_type",
                "notional_amount", "currency", "counterparty_id", "_source_row",
            ]
        )
        with patch("psycopg2.connect") as mock_connect:
            result = loader.load_trades(empty_df, _make_credentials(), "test.csv")
        self.assertEqual(result, 0)
        mock_connect.assert_not_called()


class TestLoadTradesInsertCount(unittest.TestCase):
    def _run_load(self, rowcount_return, n_rows=3):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = rowcount_return

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values") as mock_exec:
                valid_df = _make_valid_df(n_rows)
                result = loader.load_trades(valid_df, _make_credentials(), "src.csv")
                return result, mock_exec, mock_cursor, mock_conn

    def test_returns_rowcount_from_cursor(self):
        # LOGIC — function must return cursor.rowcount (actual inserts)
        result, _, _, _ = self._run_load(rowcount_return=3, n_rows=3)
        self.assertEqual(result, 3)

    def test_returns_zero_on_all_conflicts(self):
        # LOGIC — if all rows conflict, rowcount is 0 and function returns 0
        result, _, _, _ = self._run_load(rowcount_return=0, n_rows=3)
        self.assertEqual(result, 0)

    def test_partial_insert_count(self):
        # LOGIC — partial conflict: 2 of 3 rows inserted
        result, _, _, _ = self._run_load(rowcount_return=2, n_rows=3)
        self.assertEqual(result, 2)

    def test_execute_values_called_once(self):
        # LOGIC — batch insert must be called exactly once regardless of row count
        _, mock_exec, _, _ = self._run_load(rowcount_return=3, n_rows=3)
        self.assertEqual(mock_exec.call_count, 1)

    def test_execute_values_page_size_1000(self):
        # LOGIC — page_size=1000 required by TAC-6 for performance
        _, mock_exec, _, _ = self._run_load(rowcount_return=3, n_rows=3)
        _, kwargs = mock_exec.call_args
        self.assertEqual(kwargs.get("page_size", mock_exec.call_args[0][4] if len(mock_exec.call_args[0]) > 4 else None), 1000)

    def test_commit_called_on_success(self):
        # LOGIC — transaction must be committed after successful insert
        _, _, _, mock_conn = self._run_load(rowcount_return=3, n_rows=3)
        mock_conn.commit.assert_called_once()

    def test_rollback_not_called_on_success(self):
        # LOGIC — rollback must NOT be called on the happy path
        _, _, _, mock_conn = self._run_load(rowcount_return=3, n_rows=3)
        mock_conn.rollback.assert_not_called()

    def test_sql_contains_on_conflict_do_nothing(self):
        # LOGIC — idempotency requires ON CONFLICT DO NOTHING in the SQL
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured_sql = {}

        def capture_execute_values(cur, sql, rows, **kwargs):
            captured_sql["sql"] = sql

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture_execute_values):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        self.assertIn("ON CONFLICT", captured_sql["sql"].upper())
        self.assertIn("DO NOTHING", captured_sql["sql"].upper())

    def test_sql_targets_correct_conflict_columns(self):
        # LOGIC — conflict target must be (trade_id, desk_code, trade_date)
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured_sql = {}

        def capture(cur, sql, rows, **kwargs):
            captured_sql["sql"] = sql

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        sql = captured_sql["sql"]
        self.assertIn("trade_id", sql)
        self.assertIn("desk_code", sql)
        self.assertIn("trade_date", sql)

    def test_row_tuple_includes_source_file(self):
        # LOGIC — each row tuple must include source_file as the last element
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured_rows = {}

        def capture(cur, sql, rows, **kwargs):
            captured_rows["rows"] = rows

        source_file = "EQTY_2026-06-15_positions.csv"
        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture):
                loader.load_trades(_make_valid_df(1), _make_credentials(), source_file)

        self.assertEqual(captured_rows["rows"][0][-1], source_file)

    def test_row_tuple_includes_loaded_at(self):
        # LOGIC — each row tuple must include a timezone-aware loaded_at timestamp
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured_rows = {}

        def capture(cur, sql, rows, **kwargs):
            captured_rows["rows"] = rows

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        loaded_at = captured_rows["rows"][0][-2]  # second-to-last column
        self.assertIsInstance(loaded_at, datetime)
        self.assertIsNotNone(loaded_at.tzinfo)
        # LOGIC — must be ET, not UTC
        offset_str = loaded_at.strftime("%z")
        self.assertIn(offset_str, ("-0500", "-0400", "+0000"))  # ET offsets
        self.assertNotEqual(loaded_at.strftime("%Z"), "UTC")


class TestLoadTradesRollbackOnException(unittest.TestCase):
    def test_rollback_called_on_exception(self):
        # LOGIC — any exception during insert must trigger rollback
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch(
                "psycopg2.extras.execute_values",
                side_effect=Exception("DB error"),
            ):
                with self.assertRaises(Exception):
                    loader.load_trades(_make_valid_df(2), _make_credentials(), "src.csv")

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    def test_exception_is_reraised(self):
        # LOGIC — exception must propagate to caller after rollback
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock()

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch(
                "psycopg2.extras.execute_values",
                side_effect=ValueError("forced failure"),
            ):
                with self.assertRaises(ValueError):
                    loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

    def test_connection_closed_on_exception(self):
        # LOGIC — connection must be closed in finally block even on exception
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch(
                "psycopg2.extras.execute_values",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(RuntimeError):
                    loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestLoadTradesSSLMode(unittest.TestCase):
    def test_ssl_require_passed_to_connect(self):
        # LOGIC — sslmode="require" must be passed to psycopg2.connect
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
            with patch("psycopg2.extras.execute_values"):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        _, kwargs = mock_connect.call_args
        self.assertEqual(kwargs.get("sslmode"), "require")


class TestLoadTradesColumnValues(unittest.TestCase):
    def test_all_nine_columns_in_row_tuple(self):
        # LOGIC — each row tuple must have exactly 9 elements matching insert columns
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured = {}

        def capture(cur, sql, rows, **kwargs):
            captured["rows"] = rows

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        # trade_id, desk_code, trade_date, instrument_type, notional_amount,
        # currency, counterparty_id, loaded_at, source_file = 9 columns
        self.assertEqual(len(captured["rows"][0]), 9)

    def test_notional_amount_cast_to_float(self):
        # LOGIC — notional_amount in tuple must be float (not string or Decimal)
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        captured = {}

        def capture(cur, sql, rows, **kwargs):
            captured["rows"] = rows

        with patch("psycopg2.connect", return_value=mock_conn):
            with patch("psycopg2.extras.execute_values", side_effect=capture):
                loader.load_trades(_make_valid_df(1), _make_credentials(), "src.csv")

        notional = captured["rows"][0][4]  # index 4 = notional_amount
        self.assertIsInstance(notional, float)