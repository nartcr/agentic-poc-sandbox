import json
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz


class TestHandler(unittest.TestCase):
    """
    Tests for handler.py entry point.
    All AWS / boto3 calls are mocked via unittest.mock.
    """

    def _make_secret_response(self, payload: dict) -> dict:
        # BOILERPLATE — mimics the shape of a Secrets Manager get_secret_value response
        return {"SecretString": json.dumps(payload)}

    @patch.dict(os.environ, {"SECRET_NAME": "test/secret"})
    @patch("handler.boto3.client")
    def test_handler_calls_secrets_manager(self, mock_boto_client):
        # LOGIC — handler must call Secrets Manager with the secret name from env
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = self._make_secret_response({"key": "val"})
        mock_boto_client.return_value = mock_sm

        from handler import handler
        handler({"records": []}, None)

        mock_boto_client.assert_called_once_with("secretsmanager")
        mock_sm.get_secret_value.assert_called_once_with(SecretId="test/secret")

    @patch.dict(os.environ, {"SECRET_NAME": "test/secret"})
    @patch("handler.boto3.client")
    def test_handler_returns_pipeline_result(self, mock_boto_client):
        # LOGIC — handler must return the dict produced by run_pipeline
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = self._make_secret_response({})
        mock_boto_client.return_value = mock_sm

        from handler import handler

        records = [
            {"id": "1", "source": "s", "payload": "a"},
            {"id": "2", "source": "s", "payload": "b"},
        ]
        result = handler({"records": records}, None)

        self.assertIn("records_processed", result)
        self.assertEqual(result["records_processed"], 2)

    @patch.dict(os.environ, {"SECRET_NAME": "test/secret"})
    @patch("handler.boto3.client")
    def test_handler_missing_secret_name_raises(self, mock_boto_client):
        # LOGIC — SECRET_NAME env var is mandatory; absence must raise KeyError
        with patch.dict(os.environ, {}, clear=True):
            # Remove SECRET_NAME from env entirely
            os.environ.pop("SECRET_NAME", None)
            from handler import handler
            with self.assertRaises(KeyError):
                handler({}, None)

    @patch.dict(os.environ, {"SECRET_NAME": "test/secret"})
    @patch("handler.boto3.client")
    def test_handler_binary_secret_decoded(self, mock_boto_client):
        # LOGIC — SecretBinary responses must be decoded and parsed correctly
        payload = {"token": "abc123"}
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = {
            "SecretBinary": json.dumps(payload).encode("utf-8")
        }
        mock_boto_client.return_value = mock_sm

        from handler import handler
        result = handler({"records": []}, None)
        self.assertIn("records_processed", result)
        self.assertEqual(result["records_processed"], 0)

    @patch.dict(os.environ, {"SECRET_NAME": "prod/config"})
    @patch("handler.boto3.client")
    def test_handler_invalid_records_reported(self, mock_boto_client):
        # LOGIC — invalid records must be counted and not appear in output
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = self._make_secret_response({})
        mock_boto_client.return_value = mock_sm

        from handler import handler

        records = [
            {"id": "", "source": "s", "payload": "a"},   # invalid — empty id
            {"id": "2", "source": "s", "payload": "b"},  # valid
        ]
        result = handler({"records": records}, None)
        self.assertEqual(result["records_invalid"], 1)
        self.assertEqual(result["records_processed"], 1)


if __name__ == "__main__":
    unittest.main()