import json
import unittest
from unittest.mock import MagicMock, patch
import os


class TestGetSecret(unittest.TestCase):
    # BOILERPLATE — patch boto3 so no real AWS call is made

    @patch("config.boto3.client")
    def test_get_secret_returns_parsed_dict(self, mock_boto_client):
        # LOGIC — happy path: secret string is valid JSON
        mock_sm = MagicMock()
        mock_boto_client.return_value = mock_sm
        mock_sm.get_secret_value.return_value = {
            "SecretString": json.dumps({"db_password": "s3cr3t"})
        }

        from config import get_secret
        result = get_secret("my/secret")

        mock_boto_client.assert_called_once_with("secretsmanager")
        mock_sm.get_secret_value.assert_called_once_with(SecretId="my/secret")
        self.assertEqual(result, {"db_password": "s3cr3t"})

    @patch("config.boto3.client")
    def test_get_secret_missing_secret_string_returns_empty(self, mock_boto_client):
        # LOGIC — SecretString absent; should return empty dict
        mock_sm = MagicMock()
        mock_boto_client.return_value = mock_sm
        mock_sm.get_secret_value.return_value = {}

        from config import get_secret
        result = get_secret("my/secret")
        self.assertEqual(result, {})


class TestLoadConfig(unittest.TestCase):

    @patch.dict(os.environ, {}, clear=False)
    @patch("config.get_secret")
    def test_load_config_raises_when_secret_name_missing(self, mock_get_secret):
        # LOGIC — missing env var should raise EnvironmentError
        os.environ.pop("SECRET_NAME", None)
        from config import load_config
        with self.assertRaises(EnvironmentError):
            load_config()
        mock_get_secret.assert_not_called()

    @patch.dict(os.environ, {"SECRET_NAME": "prod/app/secret"})
    @patch("config.get_secret")
    def test_load_config_returns_config_dict(self, mock_get_secret):
        # LOGIC — env var present; config dict should be populated correctly
        mock_get_secret.return_value = {"api_key": "abc123"}
        from config import load_config
        result = load_config()
        self.assertEqual(result["secret_name"], "prod/app/secret")
        self.assertEqual(result["credentials"], {"api_key": "abc123"})
        mock_get_secret.assert_called_once_with("prod/app/secret")


if __name__ == "__main__":
    unittest.main()