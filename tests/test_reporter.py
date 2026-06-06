# BOILERPLATE
import datetime
import json
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import pytz

# BOILERPLATE
ET = pytz.timezone("America/Toronto")

# LOGIC — required columns that null_rates must cover (matches validator.py rules)
REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


# BOILERPLATE — build a raw DataFrame (all strings, as produced by parse_csv)
def _make_raw_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# BOILERPLATE — build a valid DataFrame (types already cast, as produced by validate_rows)
def _make_valid_df(n: int, start_notional: float = 100000.0) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "trade_id": f"TRD{i:07d}",
            "desk_code": "EQTY" if i % 2 == 0 else "FICC",
            "trade_date": datetime.date(2026, 6, 1),
            "instrument_type": "EQUITY",
            "notional_amount": start_notional + float(i * 1000),
            "currency": "USD",
            "counterparty_id": f"CP{i:05d}",
            "_source_file": "positions/EQTY_2026-06-01_positions.csv",
        })
    return pd.DataFrame(rows)


# BOILERPLATE — build a rejected DataFrame
def _make_rejected_df(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "trade_id": "",
            "desk_code": "EQTY",
            "trade_date": "2026-06-01",
            "instrument_type": "EQUITY",
            "notional_amount": "50000",
            "currency": "USD",
            "counterparty_id": f"CP{i:05d}",
            "_source_file": "positions/EQTY_2026-06-01_positions.csv",
            "_rejection_reason": "trade_id is missing or empty",
            "_source_row_number": i + 1,
        })
    return pd.DataFrame(rows)


class TestBuildReportRequiredFields(unittest.TestCase):
    # LOGIC — TAC-4: report must contain all specified fields
    def test_all_required_fields_present(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_raw_df([
            {
                "trade_id": "TRD0000001",
                "desk_code": "EQTY",
                "trade_date": "2026-06-01",
                "instrument_type": "EQUITY",
                "notional_amount": "100000",
                "currency": "USD",
                "counterparty_id": "CP00001",
                "_source_file": source_key,
            }
        ])
        valid_df = _make_valid_df(1)
        rejected_df = _make_rejected_df(0)
        load_ts = datetime.datetime.now(ET)

        report = build_report(source_key, raw_df, valid_df, rejected_df, 1, load_ts)

        expected_fields = [
            "source_file",
            "total_rows_received",
            "rows_loaded",
            "rows_rejected",
            "rows_skipped_duplicate",
            "load_timestamp",
            "desk_code_counts",
            "notional_min",
            "notional_max",
            "null_rates",
            "error_file_key",
        ]
        for field in expected_fields:
            self.assertIn(field, report, f"Missing field: {field}")

    # LOGIC — source_file matches the source_key passed in
    def test_source_file_matches_key(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_valid_df(5)
        # Rename columns back to string types for raw_df simulation
        raw_df_str = raw_df.copy()
        raw_df_str["notional_amount"] = raw_df_str["notional_amount"].astype(str)
        raw_df_str["trade_date"] = raw_df_str["trade_date"].astype(str)
        valid_df = _make_valid_df(5)
        rejected_df = _make_rejected_df(0)
        load_ts = datetime.datetime.now(ET)

        report = build_report(source_key, raw_df_str, valid_df, rejected_df, 5, load_ts)

        self.assertEqual(report["source_file"], source_key)


class TestBuildReportRowCounts(unittest.TestCase):
    # LOGIC — TAC-4: total_rows_received = len(raw_df)
    def test_total_rows_received(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_raw_df([
            {"trade_id": f"T{i}", "desk_code": "EQTY", "trade_date": "2026-06-01",
             "instrument_type": "EQ", "notional_amount": "1000", "currency": "USD",
             "counterparty_id": "CP1", "_source_file": source_key}
            for i in range(20)
        ])
        valid_df = _make_valid_df(15)
        rejected_df = _make_rejected_df(5)
        load_ts = datetime.datetime.now(ET)

        report = build_report(source_key, raw_df, valid_df, rejected_df, 12, load_ts)

        self.assertEqual(report["total_rows_received"], 20)

    # LOGIC — TAC-4: rows_loaded = rows_inserted argument (not len(valid_df))
    def test_rows_loaded_is_inserted_count_not_valid_count(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_raw_df([
            {"trade_id": f"T{i}", "desk_code": "EQTY", "trade_date": "2026-06-01",
             "instrument_type": "EQ", "notional_amount": "1000", "currency": "USD",
             "counterparty_id": "CP1", "_source_file": source_key}
            for i in range(10)
        ])
        valid_df = _make_valid_df(8)  # 8 valid rows
        rejected_df = _make_rejected_df(2)
        load_ts = datetime.datetime.now(ET)

        # Only 5 were actually inserted (3 were duplicates)
        report = build_report(source_key, raw_df, valid_df, rejected_df, 5, load_ts)

        self.assertEqual(report["rows_loaded"], 5)

    # LOGIC — TAC-4: rows_rejected = len(rejected_df)
    def test_rows_rejected(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_raw_df([
            {"trade_id": f"T{i}", "desk_code": "EQTY", "trade_date": "2026-06-01",
             "instrument_type": "EQ", "notional_amount": "1000", "currency": "USD",
             "counterparty_id": "CP1", "_source_file": source_key}
            for i in range(7)
        ])
        valid_df = _make_valid_df(4)
        rejected_df = _make_rejected_df(3)
        load_ts = datetime.datetime.now(ET)

        report = build_report(source_key, raw_df, valid_df, rejected_df, 4, load_ts)

        self.assertEqual(report["rows_rejected"], 3)

    # LOGIC — TAC-4: rows_skipped_duplicate = len(valid_df) - rows_inserted
    def test_rows_skipped_duplicate_computation(self):
        from src.reporter import build_report

        source_key = "positions/EQTY_2026-06-01_positions.csv"
        raw_df = _make_raw_df([
            {"trade_id": f"T{i}", "desk_code": "EQTY", "trade_date": "2026-06-01",
             "instrument_type": "EQ", "notional_amount": "1000", "currency": "USD",
             "counterparty_id": "CP1",