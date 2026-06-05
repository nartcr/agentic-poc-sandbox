import unittest
from unittest.mock import patch, MagicMock
import os
import json

# BOILERPLATE


class TestHandler(unittest.TestCase):

    def _make_event(self, records):
        return {"records": records}

    @patch.dict(os.environ, {"SECRET_NAME": "test-secret"})
    @patch("handler.load_secret")
    @patch("handler.process_records")
    def test_handler_returns_processed_and_count(self, mock_process, mock_load_secret):
        # LOGIC
        mock_load_secret.return_value = {"transform_prefix": "x"}
        mock_process.return_value = [{"transformed_value": "x:hello"}]

        from handler import handler
        result = handler(self._make_event([{"value": "hello"}]))

        self.assertIn("processed", result)
        self.assertIn("count", result)
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["processed"], [{"transformed_value": "x:hello"}])

    @patch.dict(os.environ, {"SECRET_NAME": "test-secret"})
    @patch("handler.load_secret")
    @patch("handler.process_records")
    def test_handler_empty_records(self, mock_process, mock_load_secret):
        # LOGIC
        mock_load_secret.return_value = {}
        mock_process.return_value = []

        from handler import handler
        result = handler(self._make_event([]))

        self.assertEqual(result["count"], 0)
        self.assertEqual(result["processed"], [])

    @patch.dict(os.environ, {}, clear=True)
    def test_handler_raises_when_no_secret_name(self):
        # LOGIC
        # Remove SECRET_NAME if present
        os.environ.pop("SECRET_NAME", None)

        from handler import handler
        with self.assertRaises(EnvironmentError):
            handler(self._make_event([{"value": "x"}]))

    @patch.dict(os.environ, {"SECRET_NAME": "test-secret"})
    @patch("handler.load_secret")
    def test_handler_raises_on_non_list_records(self, mock_load_secret):
        # LOGIC
        mock_load_secret.return_value = {}

        from handler import handler
        with self.assertRaises(TypeError):
            handler({"records": "not_a_list"})

    @patch.dict(os.environ, {"SECRET_NAME": "test-secret"})
    @patch("handler.load_secret")
    @patch("handler.process_records")
    def test_handler_passes_secret_to_process(self, mock_process, mock_load_secret):
        # LOGIC — ensure secret is forwarded from load_secret to process_records
        secret = {"transform_prefix": "forwarded"}
        mock_load_secret.return_value = secret
        mock_process.return_value = []

        from handler import handler
        handler(self._make_event([{"value": "v"}]))

        mock_process.assert_called_once_with([{"value": "v"}], secret)

    @patch.dict(os.environ, {"SECRET_NAME": "test-secret"})
    @patch("handler.load_secret")
    @patch("handler.process_records")
    def test_handler_missing_records_key_defaults_to_empty(self, mock_process, mock_load_secret):
        # LOGIC
        mock_load_secret.return_value = {}
        mock_process.return_value = []

        from handler import handler
        result = handler({})  # No 'records' key

        mock_process.assert_called_once_with([], {})
        self.assertEqual(result["count"], 0)