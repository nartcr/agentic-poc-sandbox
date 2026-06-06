# BOILERPLATE
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
import os
import sys
from datetime import datetime

# BOILERPLATE — ensure src is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# MANDATORY_COLUMNS from data contract
MANDATORY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


class TestBuildAndStoreReport(unittest.TestCase):
    """Tests for reporter.build_and_store_report — TAC-4, TAC-7."""

    # BOILERPLATE
    def setUp(self):
        os.environ["S3_BUCKET"] = "test-bucket"

    # BOILERPLATE
    def tearDown(self):
        os.environ.pop("S3_BUCKET", None)

    # BOILERPLATE
    def _make_raw_df(self, n=10, empty_trade_date_indices=None, empty_notional_indices=None):
        """Build a raw (string-typed) DataFrame simulating file_reader output."""
        # LOGIC
        rows = []
        for i in range(n):
            rows.append(
                {
                    "trade_id": f"TRD-{i:04d}",
                    "desk_code": "EQTY",
                    "trade_date": "2026-06-15",
                    "instrument_type": "EQUITY",
                    "notional_amount": str(float(1000 * (i + 1))),
                    "currency": "USD",
                    "counterparty_id": f"CP-{i:03d}",
                }
            )
        df = pd.DataFrame(rows)
        if empty_trade_date_indices:
            for idx in empty_trade_date_indices:
                df.loc[idx, "trade_date"] = ""
        if empty_notional_indices:
            for idx in empty_notional_indices:
                df.loc[idx, "notional_amount"] = ""
        return df

    # BOILERPLATE
    def _make_valid_df(self, n=10, notional_values=None):
        """Build a valid (float-typed notional_amount) DataFrame."""
        # LOGIC
        if notional_values is None:
            notional_values = [float(1000 * (i + 1)) for i in range(n)]
        return pd.DataFrame(
            {
                "trade_id": [f"TRD-{i:04d}" for i in range(n)],
                "desk_code": ["EQTY"] * n,
                "trade_date": ["2026-06-15"] * n,
                "instrument_type": ["EQUITY"] * n,
                "notional_amount": notional_values,
                "currency": ["USD"] * n,
                "counterparty_id": [f"CP-{i:03d}" for i in range(n)],
            }
        )

    # BOILERPLATE
    def _make_rejected_df(self, n=0):
        """Build a rejected DataFrame with rejection_reason column."""
        if n == 0:
            return pd.DataFrame(
                columns=MANDATORY_COLUMNS + ["rejection_reason"]
            )
        rows = []
        for i in range(n):
            rows.append(
                {
                    "trade_id": "",
                    "desk_code": "EQTY",
                    "trade_date": "2026-06-15",
                    "instrument_type": "EQUITY",
                    "notional_amount": "bad",
                    "currency": "USD",
                    "counterparty_id": f"CP-{i:03d}",
                    "rejection_reason": "trade_id: missing | notional_amount: not a valid number",
                }
            )
        return pd.DataFrame(rows)

    @patch("src.ingestion.reporter.boto3.client")
    def test_processing_timestamp_is_eastern_time(self, mock_boto_client):
        """TAC-7: processing_timestamp offset must be -04:00 or -05:00 (ET)."""
        # BOILERPLATE — mock S3 put_object
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        raw_df = self._make_raw_df(10)
        valid_df = self._make_valid_df(10)
        rejected_df = self._make_rejected_df(0)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=10,
        )

        # LOGIC — parse timestamp and assert ET offset
        ts_str = report["processing_timestamp"]
        self.assertIsInstance(ts_str, str)
        self.assertTrue(
            ts_str.endswith("-04:00") or ts_str.endswith("-05:00"),
            f"Expected ET offset (-04:00 or -05:00), got: {ts_str}",
        )

        # LOGIC — must be parseable as an ISO8601 datetime
        parsed = datetime.fromisoformat(ts_str)
        self.assertIsNotNone(parsed.tzinfo)
        offset_seconds = parsed.utcoffset().total_seconds()
        # ET is either -4h or -5h
        self.assertIn(offset_seconds, [-4 * 3600, -5 * 3600])

    @patch("src.ingestion.reporter.boto3.client")
    def test_min_and_max_notional_match_input(self, mock_boto_client):
        """TAC-4: min_notional and max_notional computed correctly from valid_df."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        notional_values = [250.0, 99999.0, 1500.5, 0.01, 777777.77]
        raw_df = self._make_raw_df(5)
        valid_df = self._make_valid_df(5, notional_values=notional_values)
        rejected_df = self._make_rejected_df(0)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=5,
        )

        # LOGIC — verify min and max
        self.assertAlmostEqual(report["min_notional"], 0.01, places=6)
        self.assertAlmostEqual(report["max_notional"], 777777.77, places=4)

    @patch("src.ingestion.reporter.boto3.client")
    def test_min_max_notional_zero_when_valid_df_empty(self, mock_boto_client):
        """TAC-4: min_notional and max_notional fall back to 0.0 when valid_df is empty."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        raw_df = self._make_raw_df(3)
        valid_df = self._make_valid_df(0)
        rejected_df = self._make_rejected_df(3)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=0,
        )

        # LOGIC — fallback to 0.0
        self.assertEqual(report["min_notional"], 0.0)
        self.assertEqual(report["max_notional"], 0.0)

    @patch("src.ingestion.reporter.boto3.client")
    def test_null_rates_computed_correctly(self, mock_boto_client):
        """TAC-4: null_rates matches (empty-string count / total rows) rounded to 6 decimal places."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        # LOGIC — 100 rows, 2 empty trade_date, 3 empty notional_amount
        raw_df = self._make_raw_df(
            100,
            empty_trade_date_indices=[0, 1],
            empty_notional_indices=[5, 10, 20],
        )
        valid_df = self._make_valid_df(95)  # 5 rejected
        rejected_df = self._make_rejected_df(5)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=95,
        )

        null_rates = report["null_rates"]

        # LOGIC — trade_date: 2 empty / 100 = 0.02
        self.assertAlmostEqual(null_rates["trade_date"], round(2 / 100, 6), places=6)

        # LOGIC — notional_amount: 3 empty / 100 = 0.03
        self.assertAlmostEqual(null_rates["notional_amount"], round(3 / 100, 6), places=6)

        # LOGIC — all other mandatory columns: 0 empty
        for col in MANDATORY_COLUMNS:
            if col not in ("trade_date", "notional_amount"):
                self.assertEqual(null_rates[col], 0.0, f"Expected 0.0 null_rate for {col}")

    @patch("src.ingestion.reporter.boto3.client")
    def test_report_counts_match_dataframe_lengths(self, mock_boto_client):
        """TAC-4: total_rows, rows_loaded, rows_rejected match inputs."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        raw_df = self._make_raw_df(20)
        valid_df = self._make_valid_df(15)
        rejected_df = self._make_rejected_df(5)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=15,
        )

        # LOGIC — counts
        self.assertEqual(report["total_rows"], 20)
        self.assertEqual(report["rows_loaded"], 15)
        self.assertEqual(report["rows_rejected"], 5)

    @patch("src.ingestion.reporter.boto3.client")
    def test_report_s3_key_and_json_written(self, mock_boto_client):
        """TAC-4: report is written to S3 at correct path with valid JSON body."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        raw_df = self._make_raw_df(10)
        valid_df = self._make_valid_df(10)
        rejected_df = self._make_rejected_df(0)

        build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=10,
        )

        # LOGIC — assert put_object was called
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]

        # LOGIC — S3 key matches data contract: reports/{desk_code}_{trade_date}_summary.json
        self.assertEqual(call_kwargs["Bucket"], "test-bucket")
        self.assertEqual(call_kwargs["Key"], "reports/EQTY_2026-06-15_summary.json")

        # LOGIC — Body must be valid JSON
        body = call_kwargs["Body"]
        parsed = json.loads(body)
        self.assertIn("source_file", parsed)
        self.assertIn("desk_code", parsed)
        self.assertIn("trade_date", parsed)
        self.assertIn("processing_timestamp", parsed)
        self.assertIn("null_rates", parsed)

    @patch("src.ingestion.reporter.boto3.client")
    def test_desk_code_counts_grouped_correctly(self, mock_boto_client):
        """TAC-4: desk_code_counts reflects groupby count from valid_df."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        # LOGIC — mix of two desk codes in valid_df
        valid_df = pd.DataFrame(
            {
                "trade_id": [f"T{i}" for i in range(10)],
                "desk_code": ["EQTY"] * 7 + ["FICC"] * 3,
                "trade_date": ["2026-06-15"] * 10,
                "instrument_type": ["EQUITY"] * 10,
                "notional_amount": [1000.0] * 10,
                "currency": ["USD"] * 10,
                "counterparty_id": ["CP-001"] * 10,
            }
        )
        raw_df = self._make_raw_df(10)
        rejected_df = self._make_rejected_df(0)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=10,
        )

        # LOGIC — counts per desk code
        self.assertEqual(report["desk_code_counts"]["EQTY"], 7)
        self.assertEqual(report["desk_code_counts"]["FICC"], 3)

    @patch("src.ingestion.reporter.boto3.client")
    def test_source_file_and_metadata_fields_correct(self, mock_boto_client):
        """TAC-4: source_file, desk_code, trade_date scalar fields correct in report."""
        # BOILERPLATE
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        from src.ingestion.reporter import build_and_store_report

        raw_df = self._make_raw_df(5)
        valid_df = self._make_valid_df(5)
        rejected_df = self._make_rejected_df(0)

        report = build_and_store_report(
            s3_bucket="test-bucket",
            source_s3_key="inbound/EQTY_2026-06-15_positions.csv",
            desk_code="EQTY",
            trade_date="2026-06-15",
            raw_df=raw_df,
            valid_df=valid_df,
            rejected_df=rejected_df,
            rows_inserted=5,
        )

        # LOGIC
        self.assertEqual(report["source_file"], "inbound/EQTY_2026-06-15_positions.csv")
        self.assertEqual(report["desk_code"], "EQTY")
        self.assertEqual(report["trade_date"], "2026-06-15")


if __name__ == "__main__":
    unittest.main()