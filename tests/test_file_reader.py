# BOILERPLATE
import io
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd


class TestFileReader(unittest.TestCase):

    def setUp(self):
        if "config" in sys.modules:
            del sys.modules["config"]
        env = {
            "S3_BUCKET": "test-bucket", "S3_INPUT_PREFIX": "positions/",
            "S3_REPORTS_PREFIX": "reports/", "S3_ERRORS_PREFIX": "errors/",
            "DB_SECRET_ID": "sid", "SNS_TOPIC_ARN_SUCCESS": "arn:s",
            "SNS_TOPIC_ARN_FAILURE": "arn:f", "AWS_REGION": "us-east-1",
        }
        self._env_patch = patch.dict(os.environ, env, clear=True)
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()
        for mod in ["config", "file_reader", "exceptions"]:
            if mod in sys.modules:
                del sys.modules[mod]

    def _make_s3_mock(self, csv_content: str):
        mock_client = MagicMock()
        mock_client.get_object.return_value = {
            "Body": io.BytesIO(csv_content.encode("utf-8"))
        }
        return mock_client

    # LOGIC
    def test_returns_dataframe_and_source_filename(self):
        csv = "trade_id,desk_code,trade_date,instrument_type,notional_amount,currency,counterparty_id\nT001,EQTY,2024-01-15,SWAP,1000000,USD,CP01\n"
        mock_client = self._make_s3_mock(csv)
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            df, name = file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(name, "EQTY_2024-01-15_positions.csv")
        self.assertEqual(len(df), 1)

    def test_all_columns_read_as_string(self):
        csv = "trade_id,desk_code,trade_date,instrument_type,notional_amount,currency,counterparty_id\nT001,EQTY,2024-01-15,SWAP,1000000,USD,CP01\n"
        mock_client = self._make_s3_mock(csv)
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            df, _ = file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")
        for col in df.columns:
            self.assertEqual(df[col].dtype, object, f"Column {col} should be str/object dtype")

    def test_desk_code_and_trade_date_in_attrs(self):
        csv = "trade_id,desk_code,trade_date,instrument_type,notional_amount,currency,counterparty_id\nT001,EQTY,2024-01-15,SWAP,1000000,USD,CP01\n"
        mock_client = self._make_s3_mock(csv)
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            df, _ = file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")
        self.assertEqual(df.attrs["desk_code"], "EQTY")
        self.assertEqual(df.attrs["trade_date"], "2024-01-15")

    def test_raises_file_read_error_on_s3_failure(self):
        import botocore.exceptions
        mock_client = MagicMock()
        mock_client.get_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "not found"}}, "GetObject"
        )
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            from exceptions import FileReadError
            with self.assertRaises(FileReadError):
                file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")

    def test_raises_file_read_error_on_empty_file(self):
        mock_client = self._make_s3_mock("   ")
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            from exceptions import FileReadError
            with self.assertRaises(FileReadError) as ctx:
                file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")
        self.assertIn("empty", str(ctx.exception).lower())

    def test_raises_file_read_error_on_bad_filename_pattern(self):
        mock_client = self._make_s3_mock("a,b\n1,2\n")
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            from exceptions import FileReadError
            with self.assertRaises(FileReadError):
                file_reader.read_position_file("test-bucket", "positions/BADFILENAME.csv")

    def test_multiple_rows_parsed(self):
        csv = "trade_id,desk_code,trade_date,instrument_type,notional_amount,currency,counterparty_id\nT001,EQTY,2024-01-15,SWAP,1000000,USD,CP01\nT002,EQTY,2024-01-15,BOND,500000,EUR,CP02\n"
        mock_client = self._make_s3_mock(csv)
        with patch("boto3.client", return_value=mock_client):
            import file_reader
            df, _ = file_reader.read_position_file("test-bucket", "positions/EQTY_2024-01-15_positions.csv")
        self.assertEqual(len(df), 2)


if __name__ == "__main__":
    unittest.main()