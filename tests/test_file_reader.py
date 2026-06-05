# BOILERPLATE
import io
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from file_reader import read_csv_from_s3


class TestReadCsvFromS3(unittest.TestCase):

    def _make_s3_response(self, csv_content: str) -> dict:
        # BOILERPLATE — construct a minimal mock get_object response
        body_mock = MagicMock()
        body_mock.read.return_value = csv_content.encode("utf-8")
        return {"Body": body_mock}

    @patch("file_reader.boto3.client")
    def test_returns_dataframe_and_source_file_name(self, mock_boto_client):
        # LOGIC — verify basic return shape: a DataFrame and the basename of the key
        csv = "trade_id,desk_code,trade_date\nT001,EQTY,2026-06-15\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        df, source_file = read_csv_from_s3("my-bucket", "positions/EQTY_2026-06-15_positions.csv")

        self.assertEqual(source_file, "EQTY_2026-06-15_positions.csv")
        self.assertIsInstance(df, pd.DataFrame)

    @patch("file_reader.boto3.client")
    def test_all_columns_read_as_string(self, mock_boto_client):
        # LOGIC — dtype=str must be applied so that numeric-looking values are not auto-cast
        csv = "trade_id,notional_amount\nT001,1000000\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        df, _ = read_csv_from_s3("my-bucket", "positions/test.csv")

        self.assertEqual(df["notional_amount"].dtype, object)  # object = string in pandas
        self.assertEqual(df["notional_amount"].iloc[0], "1000000")

    @patch("file_reader.boto3.client")
    def test_source_row_column_is_1_based(self, mock_boto_client):
        # LOGIC — _source_row must start at 1 for the first data row
        csv = "trade_id\nT001\nT002\nT003\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        df, _ = read_csv_from_s3("my-bucket", "positions/test.csv")

        self.assertIn("_source_row", df.columns)
        self.assertEqual(list(df["_source_row"]), [1, 2, 3])

    @patch("file_reader.boto3.client")
    def test_source_row_values_for_multirow_file(self, mock_boto_client):
        # LOGIC — source row numbers must always correspond to original file position
        rows = "\n".join(f"T{i:03d}" for i in range(1, 1001))
        csv = f"trade_id\n{rows}\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        df, _ = read_csv_from_s3("my-bucket", "positions/big.csv")

        self.assertEqual(len(df), 1000)
        self.assertEqual(df["_source_row"].iloc[0], 1)
        self.assertEqual(df["_source_row"].iloc[999], 1000)

    @patch("file_reader.boto3.client")
    def test_calls_get_object_with_correct_bucket_and_key(self, mock_boto_client):
        # LOGIC — must pass exact bucket name and key to S3
        csv = "trade_id\nT001\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        read_csv_from_s3("target-bucket", "positions/EQTY_2026-06-15_positions.csv")

        mock_s3.get_object.assert_called_once_with(
            Bucket="target-bucket", Key="positions/EQTY_2026-06-15_positions.csv"
        )

    @patch("file_reader.boto3.client")
    def test_empty_strings_not_coerced_to_nan(self, mock_boto_client):
        # LOGIC — keep_default_na=False must preserve empty strings as "" not NaN
        csv = "trade_id,desk_code\nT001,\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        df, _ = read_csv_from_s3("my-bucket", "positions/test.csv")

        # empty field must come through as empty string, not NaN
        self.assertEqual(df["desk_code"].iloc[0], "")

    @patch("file_reader.boto3.client")
    def test_source_file_is_basename_only(self, mock_boto_client):
        # LOGIC — nested prefix must be stripped; only the filename is returned
        csv = "trade_id\nT001\n"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = self._make_s3_response(csv)
        mock_boto_client.return_value = mock_s3

        _, source_file = read_csv_from_s3("my-bucket", "a/b/c/deep_file.csv")

        self.assertEqual(source_file, "deep_file.csv")


class TestReadCsvFromS3ClientCreation(unittest.TestCase):

    @patch("file_reader.boto3.client")
    def test_creates_s3_client_without_explicit_credentials(self, mock_boto_client):
        # LOGIC — boto3.client must be called as client("s3") with no access key args
        csv = "trade_id\nT001\n"
        mock_s3 = MagicMock()
        body = MagicMock()
        body.read.return_value = csv.encode("utf-8")
        mock_s3.get_object.return_value = {"Body": body}
        mock_boto_client.return_value = mock_s3

        read_csv_from_s3("my-bucket", "positions/test.csv")

        mock_boto_client.assert_called_once_with("s3")


if __name__ == "__main__":
    unittest.main()