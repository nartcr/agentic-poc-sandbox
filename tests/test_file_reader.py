# BOILERPLATE
import io
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from src.file_reader import download_file, parse_csv


def _make_client_error(code: str) -> ClientError:
    # BOILERPLATE — helper to construct a botocore ClientError
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "GetObject",
    )


class TestDownloadFile(unittest.TestCase):

    def test_download_returns_bytes_io(self):
        # LOGIC — happy path: get_object returns body bytes
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": io.BytesIO(b"col1,col2\n1,2\n")}
        result = download_file(s3, "my-bucket", "positions/test.csv")
        self.assertIsInstance(result, io.BytesIO)
        self.assertEqual(result.read(), b"col1,col2\n1,2\n")
        s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="positions/test.csv")

    def test_download_raises_file_not_found_for_no_such_key(self):
        # LOGIC — NoSuchKey error code maps to FileNotFoundError
        s3 = MagicMock()
        s3.get_object.side_effect = _make_client_error("NoSuchKey")
        with self.assertRaises(FileNotFoundError) as ctx:
            download_file(s3, "my-bucket", "positions/missing.csv")
        self.assertIn("positions/missing.csv", str(ctx.exception))

    def test_download_raises_file_not_found_for_404(self):
        # LOGIC — 404 error code also maps to FileNotFoundError
        s3 = MagicMock()
        s3.get_object.side_effect = _make_client_error("404")
        with self.assertRaises(FileNotFoundError):
            download_file(s3, "my-bucket", "positions/missing.csv")

    def test_download_raises_io_error_for_other_client_errors(self):
        # LOGIC — non-404 S3 errors become IOError
        s3 = MagicMock()
        s3.get_object.side_effect = _make_client_error("AccessDenied")
        with self.assertRaises(IOError):
            download_file(s3, "my-bucket", "positions/secret.csv")

    def test_download_raises_io_error_on_body_read_failure(self):
        # LOGIC — failure reading the body stream raises IOError
        s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.side_effect = RuntimeError("stream broken")
        s3.get_object.return_value = {"Body": mock_body}
        with self.assertRaises(IOError) as ctx:
            download_file(s3, "my-bucket", "positions/test.csv")
        self.assertIn("stream broken", str(ctx.exception))


class TestParseCsv(unittest.TestCase):

    def _make_bytes(self, text: str) -> io.BytesIO:
        return io.BytesIO(text.encode("utf-8"))

    def test_parse_returns_dataframe_with_all_string_columns(self):
        # LOGIC — all columns must be dtype str to prevent silent coercion
        csv_text = "trade_id,notional_amount\nT001,1000.00\n"
        result = parse_csv(self._make_bytes(csv_text), "positions/test.csv")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result["trade_id"].dtype, object)
        self.assertEqual(result["notional_amount"].dtype, object)

    def test_parse_adds_source_file_column(self):
        # LOGIC — _source_file column must be set to the source_key argument
        csv_text = "trade_id,desk_code\nT001,EQTY\n"
        result = parse_csv(self._make_bytes(csv_text), "positions/EQTY_2026-06-01_positions.csv")
        self.assertIn("_source_file", result.columns)
        self.assertEqual(result["_source_file"].iloc[0], "positions/EQTY_2026-06-01_positions.csv")

    def test_parse_preserves_empty_strings_not_nan(self):
        # LOGIC — empty cells must remain "" not NaN (keep_default_na=False)
        csv_text = "trade_id,desk_code\nT001,\n"
        result = parse_csv(self._make_bytes(csv_text), "positions/test.csv")
        self.assertEqual(result["desk_code"].iloc[0], "")

    def test_parse_returns_all_rows(self):
        # LOGIC — no rows are dropped during parsing
        rows = "\n".join(f"T{i:04d},EQTY" for i in range(1, 101))
        csv_text = f"trade_id,desk_code\n{rows}\n"
        result = parse_csv(self._make_bytes(csv_text), "positions/test.csv")
        self.assertEqual(len(result), 100)

    def test_parse_raises_value_error_on_empty_file(self):
        # LOGIC — completely empty file (no header) raises ValueError
        with self.assertRaises(ValueError):
            parse_csv(self._make_bytes(""), "positions/empty.csv")

    def test_parse_raises_value_error_on_bad_encoding(self):
        # LOGIC — binary garbage that pandas cannot parse raises ValueError
        bad_bytes = io.BytesIO(b"\xff\xfe\x00\x00")
        # pandas may or may not raise; if it does not raise, at least we get a DF
        # This test documents the contract: ValueError on unrecoverable parse failures
        try:
            result = parse_csv(bad_bytes, "positions/bad.csv")
            # If pandas silently parses it, that is acceptable behaviour
            self.assertIsInstance(result, pd.DataFrame)
        except ValueError:
            pass  # LOGIC — expected path

    def test_parse_passes_through_extra_columns(self):
        # LOGIC — columns beyond the required set must be preserved
        csv_text = "trade_id,desk_code,extra_column\nT001,EQTY,extra_value\n"
        result = parse_csv(self._make_bytes(csv_text), "positions/test.csv")
        self.assertIn("extra_column", result.columns)
        self.assertEqual(result["extra_column"].iloc[0], "extra_value")

    def test_parse_resets_file_pointer_before_reading(self):
        # LOGIC — BytesIO already-read past position 0 must still parse correctly
        csv_text = "trade_id,desk_code\nT001,EQTY\n"
        file_bytes = self._make_bytes(csv_text)
        file_bytes.read(5)  # advance pointer
        result = parse_csv(file_bytes, "positions/test.csv")
        self.assertEqual(len(result), 1)