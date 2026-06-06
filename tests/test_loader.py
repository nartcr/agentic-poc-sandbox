# BOILERPLATE
import datetime
import unittest
from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytz

# BOILERPLATE
ET = pytz.timezone("America/Toronto")

# BOILERPLATE — helper to build a valid DataFrame matching app.daily_trades schema
def _make_valid_df(n: int, start_trade_id: int = 1) -> pd.DataFrame:
    # LOGIC
    rows = []
    for i in range(n):
        rows.append({
            "trade_id": f"TRD{start_trade_id + i:07d}",
            "desk_code": "EQTY",
            "trade_date": datetime.date(2026, 6, 1),
            "instrument_type": "EQUITY",
            "notional_amount": float(100000 + i),
            "currency": "USD",
            "counterparty_id": f"CP{i:05d}",
            "_source_file": "positions/EQTY_2026-06-01_positions.csv",
        })
    return pd.DataFrame(rows)


# BOILERPLATE — helper to build mock psycopg2 connection with configurable rowcount
def _make_mock_conn(rowcount: int):
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda s: mock_cursor
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = rowcount

    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


class TestLoadTradesInsertCount(unittest.TestCase):
    # LOGIC — TAC-1: 1000-row insert returns rows_inserted == 1000
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_insert_1000_rows_returns_1000(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1000)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1000)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        result = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertEqual(result, 1000)
        mock_connect.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.rollback.assert_not_called()

    # LOGIC — TAC-3: second insert of same rows returns rows_inserted == 0
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_duplicate_insert_returns_zero(self, mock_connect, mock_execute_values):
        # LOGIC — first call rowcount=1000, second call rowcount=0
        mock_conn_1, mock_cursor_1 = _make_mock_conn(rowcount=1000)
        mock_conn_2, mock_cursor_2 = _make_mock_conn(rowcount=0)
        mock_connect.side_effect = [mock_conn_1, mock_conn_2]

        valid_df = _make_valid_df(1000)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        result_1 = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)
        result_2 = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertEqual(result_1, 1000)
        self.assertEqual(result_2, 0)

    # LOGIC — TAC-3: partial overlap: 1000 existing rows + 200 new rows = 200 inserted
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_partial_overlap_inserts_only_new_rows(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=200)
        mock_connect.return_value = mock_conn

        # 1000 rows: first 800 are "existing" (will conflict), last 200 are new
        valid_df = _make_valid_df(1000)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        result = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertEqual(result, 200)


class TestLoadTradesConnectionParams(unittest.TestCase):
    # LOGIC — TAC-8: credentials dict fields map to psycopg2.connect parameters
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_connect_called_with_correct_credentials(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "aurora-host.example.com",
            "port": "5432",
            "dbname": "rfdh",
            "username": "rfdh_app_user",
            "password": "s3cr3t",
        }

        from src.loader import load_trades
        load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        mock_connect.assert_called_once_with(
            host="aurora-host.example.com",
            port=5432,
            dbname="rfdh",
            user="rfdh_app_user",
            password="s3cr3t",
        )

    # LOGIC — TAC-8: port is cast to int (secret stores port as string)
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_port_cast_to_int(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        call_kwargs = mock_connect.call_args[1]
        self.assertIsInstance(call_kwargs["port"], int)
        self.assertEqual(call_kwargs["port"], 5432)


class TestLoadTradesTimezone(unittest.TestCase):
    # LOGIC — TAC-7: loaded_at must be ET-aware; UTC datetime must raise AssertionError
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_loaded_at_utc_raises_assertion_error(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1)
        loaded_at_utc = datetime.datetime.now(datetime.timezone.utc)  # UTC, not ET
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        with self.assertRaises(AssertionError):
            load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at_utc)

    # LOGIC — TAC-7: naive datetime (no timezone) must raise AssertionError
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_loaded_at_naive_raises_assertion_error(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1)
        loaded_at_naive = datetime.datetime(2026, 6, 1, 20, 0, 0)  # no tzinfo
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        with self.assertRaises(AssertionError):
            load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at_naive)

    # LOGIC — TAC-7: valid ET-aware datetime is accepted without error
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_loaded_at_et_accepted(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=5)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(5)
        loaded_at_et = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        # LOGIC — should not raise
        result = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at_et)
        self.assertEqual(result, 5)

    # LOGIC — TAC-7: loaded_at in the tuples passed to execute_values is ET-aware
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_loaded_at_in_tuples_is_et_aware(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=3)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(3)
        loaded_at_et = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at_et)

        # LOGIC — extract the argsval list passed to execute_values
        call_args = mock_execute_values.call_args
        # execute_values(cursor, sql, argslist) — argslist is the third positional arg
        argslist = call_args[0][2]
        self.assertEqual(len(argslist), 3)
        for row_tuple in argslist:
            # loaded_at is index 7 in the tuple: (trade_id, desk_code, trade_date,
            # instrument_type, notional_amount, currency, counterparty_id, loaded_at, source_file)
            ts = row_tuple[7]
            self.assertIsNotNone(ts.tzinfo)
            # LOGIC — verify offset is -04:00 or -05:00 (ET), not UTC
            offset_hours = ts.utcoffset().total_seconds() / 3600
            self.assertIn(offset_hours, (-4.0, -5.0))


class TestLoadTradesSQLContent(unittest.TestCase):
    # LOGIC — verifies the SQL contains the correct table name and ON CONFLICT clause
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_sql_contains_correct_table_and_conflict(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=1)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(1)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        call_args = mock_execute_values.call_args
        sql = call_args[0][1]  # second positional arg is the SQL string
        self.assertIn("app.daily_trades", sql)
        self.assertIn("ON CONFLICT", sql)
        self.assertIn("DO NOTHING", sql)
        self.assertIn("trade_id", sql)
        self.assertIn("desk_code", sql)
        self.assertIn("trade_date", sql)

    # LOGIC — verifies tuple structure: 9 fields per row in correct order
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_tuple_structure_has_nine_fields(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=2)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(2)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        call_args = mock_execute_values.call_args
        argslist = call_args[0][2]
        for row_tuple in argslist:
            self.assertEqual(len(row_tuple), 9)


class TestLoadTradesRollbackOnError(unittest.TestCase):
    # LOGIC — exception during execute_values causes rollback and re-raise
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_rollback_on_execute_error(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=0)
        mock_connect.return_value = mock_conn
        mock_execute_values.side_effect = Exception("DB write failed")

        valid_df = _make_valid_df(5)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        with self.assertRaises(Exception) as ctx:
            load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertIn("DB write failed", str(ctx.exception))
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    # LOGIC — connection error propagates as-is
    @patch("src.loader.psycopg2.connect")
    def test_connection_error_propagates(self, mock_connect):
        mock_connect.side_effect = Exception("Connection refused")

        valid_df = _make_valid_df(5)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "bad-host",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        with self.assertRaises(Exception) as ctx:
            load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertIn("Connection refused", str(ctx.exception))


class TestLoadTradesEmptyDataFrame(unittest.TestCase):
    # LOGIC — empty valid_df inserts 0 rows; execute_values still called (no early exit required,
    # but if it is, rowcount fallback to 0 is correct)
    @patch("src.loader.psycopg2.extras.execute_values")
    @patch("src.loader.psycopg2.connect")
    def test_empty_dataframe_returns_zero(self, mock_connect, mock_execute_values):
        mock_conn, mock_cursor = _make_mock_conn(rowcount=0)
        mock_connect.return_value = mock_conn

        valid_df = _make_valid_df(0)
        loaded_at = datetime.datetime.now(ET)
        credentials = {
            "host": "localhost",
            "port": "5432",
            "dbname": "testdb",
            "username": "user",
            "password": "pass",
        }

        from src.loader import load_trades
        result = load_trades(valid_df, credentials, "positions/EQTY_2026-06-01_positions.csv", loaded_at)

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()