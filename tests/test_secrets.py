# BOILERPLATE
import json
import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import botocore.exceptions


class TestGetDbCredentials(unittest.TestCase):

    def setUp(self):
        # BOILERPLATE — ensure config module is loaded with required env vars
        if "config" in sys.modules:
            del sys.modules["config"]
        env = {
            "S3_BUCKET": "b", "S3_INPUT_PREFIX": "i/", "S3_REPORTS_PREFIX": "r/",
            "S3_ERRORS_PREFIX": "e/", "DB_SECRET_ID": "sid",
            "SNS_TOPIC_ARN_SUCCESS": "arn:success", "SNS_TOPIC_ARN_FAILURE": "arn:failure",
            "AWS_REGION": "us-east-1",
        }
        self._env_patch = patch.dict(os.environ, env, clear=True)
        self._env_patch.start()
        import config  # noqa: F401

    def tearDown(self):
        self._env_patch.stop()
        for mod in ["config", "secrets"]:
            if mod in sys.modules:
                del sys.modules[mod]

    # LOGIC
    def test_returns_credential_dict(self):
        secret_payload = {
            "host": "db.example.com",
            "port": 5432,
            "dbname": "trades",
            "username": "svc_user",
            "password": "s3cr3t",
        }
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret_payload)
        }
        with patch("boto3.client", return_value=mock_client):
            import secrets as sec
            result = sec.get_db_credentials("my-secret-id")
        self.assertEqual(result["host"], "db.example.com")
        self.assertEqual(result["username"], "svc_user")
        self.assertEqual(result["password"], "s3cr3t")

    def test_raises_runtime_error_on_client_error(self):
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
            "GetSecretValue",
        )
        with patch("boto3.client", return_value=mock_client):
            import secrets as sec
            with self.assertRaises(RuntimeError) as ctx:
                sec.get_db_credentials("bad-secret")
        self.assertIn("ResourceNotFoundException", str(ctx.exception))
        # LOGIC — must not expose credential values
        self.assertNotIn("password", str(ctx.exception))

    def test_raises_runtime_error_on_missing_keys(self):
        secret_payload = {"host": "db.example.com"}  # missing port, dbname, username, password
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(secret_payload)
        }
        with patch("boto3.client", return_value=mock_client):
            import secrets as sec
            with self.assertRaises(RuntimeError) as ctx:
                sec.get_db_credentials("incomplete-secret")
        self.assertIn("missing required credential fields", str(ctx.exception))

    def test_raises_runtime_error_on_invalid_json(self):
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {"SecretString": "not-json{{"}
        with patch("boto3.client", return_value=mock_client):
            import secrets as sec
            with self.assertRaises(RuntimeError):
                sec.get_db_credentials("bad-json-secret")


if __name__ == "__main__":
    unittest.main()