# BOILERPLATE
import io
import unittest
from unittest.mock import MagicMock, patch, call

import pandas as pd

import error_writer


def _make_rejected_df():
    # LOGIC — construct a rejected DataFrame matching the post-validator schema
    return pd.DataFrame(
        [
            {
                "_source_row": 1,
                "trade_id": "",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY",
                "notional_amount": "1000.00",
                "currency": "USD",
                "counterparty_id": "CP001",
                "rejection_reason": "trade_id is missing or empty",
            },
            {
                "_source_row": 3,
                "trade_id": "T0002",
                "desk_code": "EQTY",
                "trade_date": "15-06-2026",
                "instrument_type": "EQUITY",
                "notional_amount": "2000.00",
                "currency": "USD",
                "counterparty_id": "CP002",
                "rejection_reason": "trade_date is not a valid date (expected YYYY-MM-DD)",
            },
            {
                "_source_row": 5,
                "trade_id": "T0003",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY",
                "notional_amount": "abc",
                "currency": "USD",
                "counterparty_id": "CP003",
                "rejection_reason": "notional_amount is not numeric",
            },
            {
                "_source_row": 7,
                "trade_id": "T0004",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY",
                "notional_amount": "4000.00",
                "currency": "USDD",
                "counterparty_id": "CP004",
                "rejection_reason": "currency must be a 3-letter ISO code",
            },
            {
                "_source_row": 9,
                "trade_id": "T0005",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY",
                "notional_amount": "5000.00",
                "currency": "GBP",
                "counterparty_id": "",
                "rejection_reason": "counterparty_id is missing or empty",
            },
        ]
    )


class TestErrorKeyDerivation(unittest.TestCase):
    def test_key_derived_from_source_key_basename(self):
        # LOGIC — error key must use stem of source key + _errors.csv
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            key = error_writer.write_error_file(
                rejected_df,
                bucket="test-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        self.assertEqual(key, "errors/EQTY_2026-06-15_positions_errors.csv")

    def test_key_with_no_subdirectory_in_source(self):
        # LOGIC — source key without prefix should still derive correct error key
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            key = error_writer.write_error_file(
                rejected_df,
                bucket="test-bucket",
                source_key="EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        self.assertEqual(key, "errors/EQTY_2026-06-15_positions_errors.csv")

    def test_custom_errors_prefix(self):
        # LOGIC — errors_prefix env var value is used verbatim as prefix
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            key = error_writer.write_error_file(
                rejected_df,
                bucket="test-bucket",
                source_key="positions/DESK_2026-01-01_positions.csv",
                errors_prefix="rejected/",
            )

        self.assertEqual(key, "rejected/DESK_2026-01-01_positions_errors.csv")


class TestErrorFileS3Upload(unittest.TestCase):
    def test_put_object_called_with_correct_bucket_and_key(self):
        # LOGIC — must upload to the exact derived error key in the correct bucket
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertEqual(call_kwargs["Bucket"], "my-bucket")
        self.assertEqual(call_kwargs["Key"], "errors/EQTY_2026-06-15_positions_errors.csv")

    def test_put_object_content_type_text_csv(self):
        # LOGIC — ContentType must be text/csv
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertEqual(call_kwargs["ContentType"], "text/csv")

    def test_put_object_body_is_bytes(self):
        # LOGIC — Body must be bytes for S3 put_object
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        call_kwargs = mock_s3.put_object.call_args[1]
        self.assertIsInstance(call_kwargs["Body"], bytes)

    def test_put_object_called_exactly_once(self):
        # LOGIC — exactly one S3 call per invocation
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        self.assertEqual(mock_s3.put_object.call_count, 1)


class TestErrorCSVContent(unittest.TestCase):
    def _get_uploaded_csv(self, source_key="positions/EQTY_2026-06-15_positions.csv"):
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key=source_key,
                errors_prefix="errors/",
            )

        body_bytes = mock_s3.put_object.call_args[1]["Body"]
        return pd.read_csv(io.BytesIO(body_bytes), dtype=str)

    def test_csv_has_correct_row_count(self):
        # LOGIC — CSV must have exactly 5 data rows (matching rejected_df)
        df = self._get_uploaded_csv()
        self.assertEqual(len(df), 5)

    def test_csv_has_all_required_columns(self):
        # LOGIC — all 9 columns from data contract must be present
        df = self._get_uploaded_csv()
        expected_columns = [
            "_source_row", "trade_id", "desk_code", "trade_date",
            "instrument_type", "notional_amount", "currency",
            "counterparty_id", "rejection_reason",
        ]
        for col in expected_columns:
            self.assertIn(col, df.columns, f"Column '{col}' missing from error CSV")

    def test_csv_column_order(self):
        # LOGIC — columns must appear in the exact order specified by the data contract
        df = self._get_uploaded_csv()
        expected_columns = [
            "_source_row", "trade_id", "desk_code", "trade_date",
            "instrument_type", "notional_amount", "currency",
            "counterparty_id", "rejection_reason",
        ]
        self.assertEqual(list(df.columns), expected_columns)

    def test_rejection_reasons_preserved(self):
        # LOGIC — rejection_reason strings must match exactly (TAC-2)
        df = self._get_uploaded_csv()
        reasons = df["rejection_reason"].tolist()
        self.assertIn("trade_id is missing or empty", reasons)
        self.assertIn("trade_date is not a valid date (expected YYYY-MM-DD)", reasons)
        self.assertIn("notional_amount is not numeric", reasons)
        self.assertIn("currency must be a 3-letter ISO code", reasons)
        self.assertIn("counterparty_id is missing or empty", reasons)

    def test_source_row_numbers_preserved(self):
        # LOGIC — _source_row must retain original 1-based file row numbers
        df = self._get_uploaded_csv()
        source_rows = [int(x) for x in df["_source_row"].tolist()]
        self.assertEqual(sorted(source_rows), [1, 3, 5, 7, 9])

    def test_csv_is_utf8_encoded(self):
        # LOGIC — output must be UTF-8 bytes (required by data contract)
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        body_bytes = mock_s3.put_object.call_args[1]["Body"]
        # Should not raise on UTF-8 decode
        decoded = body_bytes.decode("utf-8")
        self.assertIn("rejection_reason", decoded)


class TestErrorWriterReturnValue(unittest.TestCase):
    def test_returns_full_s3_key(self):
        # LOGIC — return value must be the complete S3 key (prefix + filename)
        rejected_df = _make_rejected_df()
        with patch("boto3.client") as mock_boto:
            mock_s3 = MagicMock()
            mock_boto.return_value = mock_s3

            result = error_writer.write_error_file(
                rejected_df,
                bucket="my-bucket",
                source_key="positions/EQTY_2026-06-15_positions.csv",
                errors_prefix="errors/",
            )

        self.assertEqual(result, "errors/EQTY_2026-06-15_positions_errors.csv")


if __name__ == "__main__":
    unittest.main()