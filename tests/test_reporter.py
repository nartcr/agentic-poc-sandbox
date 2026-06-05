# BOILERPLATE
import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz

import reporter


class TestExtractFilenameParts(unittest.TestCase):
    # LOGIC — filename parsing

    def test_standard_filename_parsed_correctly(self):
        desk, trade_date = reporter._extract_filename_parts(
            "positions/EQTY_2026-06-15_positions.csv"
        )
        self.assertEqual(desk, "EQTY")
        self.assertEqual(trade_date, "2026-06-15")

    def test_basename_only_parsed_correctly(self):
        desk, trade_date = reporter._extract_filename_parts(
            "FIXED_2025-12-31_positions.csv"
        )
        self.assertEqual(desk, "FIXED")
        self.assertEqual(trade_date, "2025-12-31")

    def test_unrecognised_pattern_returns_unknown(self):
        desk, trade_date = reporter._extract_filename_parts("garbage_file.csv")
        self.assertEqual(desk, "UNKNOWN")
        self.assertEqual(trade_date, "UNKNOWN")


class TestBuildReport(unittest.TestCase):
    # LOGIC — report dict computation

    def _make_raw_df(self):
        return pd.DataFrame(
            {
                "trade_id": ["T001", "T002", "T003", None],
                "desk_code": ["EQTY", "EQTY", "EQTY", "EQTY"],
                "trade_date": ["2026-06-15", "2026-06-15", "2026-06-15", None],
                "instrument_type": ["SWAP", "SWAP", "SWAP", "SWAP"],
                "notional_amount": ["1000.0", "2000.0", "3000.0", None],
                "currency": ["USD", "EUR", "GBP", "USD"],
                "counterparty_id": ["CP1", "CP2", "CP3", "CP4"],
                "_source_row": [1, 2, 3, 4],
            }
        )

    def _make_valid_df(self):
        df = pd.DataFrame(
            {
                "trade_id": ["T001", "T002", "T003"],
                "desk_code": ["EQTY", "EQTY", "EQTY"],
                "trade_date": ["2026-06-15", "2026-06-15", "2026-06-15"],
                "instrument_type": ["SWAP", "SWAP", "SWAP"],
                "notional_amount": [1000.0, 2000.0, 3000.0],
                "currency": ["USD", "EUR", "GBP"],
                "counterparty_id": ["CP1", "CP2", "CP3"],
                "_source_row": [1, 2, 3],
            }
        )
        df["notional_amount"] = df["notional_amount"].astype("float64")
        return df

    def _make_rejected_df(self):
        return pd.DataFrame(
            {
                "trade_id": [None],
                "desk_code": ["EQTY"],
                "trade_date": [None],
                "instrument_type": ["SWAP"],
                "notional_amount": [None],
                "currency": ["USD"],
                "counterparty_id": ["CP4"],
                "_source_row": [4],
                "rejection_reason": ["trade_id is missing or empty"],
            }
        )

    def _et_now(self):
        return datetime.now(pytz.timezone("America/Toronto"))

    def test_total_rows_received_matches_raw_df(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key="errors/EQTY_2026-06-15_positions_errors.csv",
        )
        self.assertEqual(report["total_rows_received"], 4)

    def test_rows_loaded_equals_rows_inserted(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=2,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertEqual(report["rows_loaded"], 2)

    def test_rows_rejected_matches_rejected_df(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertEqual(report["rows_rejected"], 1)

    def test_rows_skipped_duplicate_computed_correctly(self):
        # valid_df has 3 rows, rows_inserted=1 → skipped=2
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=1,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertEqual(report["rows_skipped_duplicate"], 2)

    def test_notional_min_max_correct(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertAlmostEqual(report["notional_amount_min"], 1000.0)
        self.assertAlmostEqual(report["notional_amount_max"], 3000.0)

    def test_notional_min_max_null_when_valid_df_empty(self):
        raw_df = self._make_raw_df()
        valid_df = pd.DataFrame(
            columns=[
                "trade_id", "desk_code", "trade_date", "instrument_type",
                "notional_amount", "currency", "counterparty_id", "_source_row",
            ]
        )
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=0,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertIsNone(report["notional_amount_min"])
        self.assertIsNone(report["notional_amount_max"])

    def test_null_rates_seven_columns_present(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        expected_cols = {
            "trade_id", "desk_code", "trade_date", "instrument_type",
            "notional_amount", "currency", "counterparty_id",
        }
        self.assertEqual(set(report["null_rates"].keys()), expected_cols)

    def test_null_rates_values_correct(self):
        # raw_df has 4 rows; trade_id row 4 is None → null_rate = 0.25
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertAlmostEqual(report["null_rates"]["trade_id"], 0.25, places=6)
        self.assertAlmostEqual(report["null_rates"]["desk_code"], 0.0, places=6)

    def test_load_timestamp_is_et_not_utc(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = datetime.now(pytz.timezone("America/Toronto"))
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        ts_str = report["load_timestamp"]
        self.assertNotIn("+00:00", ts_str)
        self.assertFalse(ts_str.endswith("Z"))
        # ET offset is either -04:00 or -05:00
        self.assertTrue(
            "-04:00" in ts_str or "-05:00" in ts_str,
            f"Expected ET offset in timestamp, got: {ts_str}",
        )

    def test_desk_code_counts_match_valid_df(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertEqual(report["desk_code_counts"], {"EQTY": 3})

    def test_error_file_key_propagated(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key="errors/EQTY_2026-06-15_positions_errors.csv",
        )
        self.assertEqual(
            report["error_file_key"],
            "errors/EQTY_2026-06-15_positions_errors.csv",
        )

    def test_source_file_desk_code_trade_date_in_report(self):
        raw_df = self._make_raw_df()
        valid_df = self._make_valid_df()
        rejected_df = self._make_rejected_df()
        ts = self._et_now()
        report = reporter.build_report(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=3,
            load_timestamp=ts,
            error_file_key=None,
        )
        self.assertEqual(report["source_file"], "positions/EQTY_2026-06-15_positions.csv")
        self.assertEqual(report["desk_code"], "EQTY")
        self.assertEqual(report["trade_date"], "2026-06-15")


class TestWriteReport(unittest.TestCase):
    # LOGIC — S3 key derivation and upload

    @patch("reporter.boto3.client")
    def test_correct_s3_key_derived(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        report = {"source_file": "positions/EQTY_2026-06-15_positions.csv"}
        key = reporter.write_report(
            report=report,
            bucket="my-bucket",
            source_key="positions/EQTY_2026-06-15_positions.csv",
            reports_prefix="reports/",
        )
        self.assertEqual(key, "reports/EQTY_2026-06-15_positions_report.json")

    @patch("reporter.boto3.client")
    def test_put_object_called_with_correct_args(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        report = {"rows_loaded": 5}
        reporter.write_report(
            report=report,
            bucket="my-bucket",
            source_key="positions/EQTY_2026-06-15_positions.csv",
            reports_prefix="reports/",
        )

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertEqual(call_kwargs["Bucket"], "my-bucket")
        self.assertEqual(call_kwargs["Key"], "reports/EQTY_2026-06-15_positions_report.json")
        self.assertEqual(call_kwargs["ContentType"], "application/json")

    @patch("reporter.boto3.client")
    def test_uploaded_json_round_trips(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        report = {"rows_loaded": 10, "rows_rejected": 2}
        reporter.write_report(
            report=report,
            bucket="my-bucket",
            source_key="positions/EQTY_2026-06-15_positions.csv",
            reports_prefix="reports/",
        )

        call_kwargs = mock_s3.put_object.call_args[1]
        body_bytes = call_kwargs["Body"]
        parsed = json.loads(body_bytes.decode("utf-8"))
        self.assertEqual(parsed["rows_loaded"], 10)
        self.assertEqual(parsed["rows_rejected"], 2)


if __name__ == "__main__":
    unittest.main()