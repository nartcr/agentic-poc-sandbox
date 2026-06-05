# BOILERPLATE
import importlib
import os
import sys
import unittest
from unittest.mock import patch


class TestConfig(unittest.TestCase):
    """Tests that config.py raises EnvironmentError for missing variables."""

    _REQUIRED_VARS = {
        "S3_BUCKET": "test-bucket",
        "S3_INPUT_PREFIX": "positions/",
        "S3_REPORTS_PREFIX": "reports/",
        "S3_ERRORS_PREFIX": "errors/",
        "DB_SECRET_ID": "my-secret",
        "SNS_TOPIC_ARN_SUCCESS": "arn:aws:sns:us-east-1:000000000000:success",
        "SNS_TOPIC_ARN_FAILURE": "arn:aws:sns:us-east-1:000000000000:failure",
        "AWS_REGION": "us-east-1",
    }

    def _reload_config(self, env_overrides: dict):
        """Remove cached config module and reload with given env."""
        if "config" in sys.modules:
            del sys.modules["config"]
        with patch.dict(os.environ, env_overrides, clear=True):
            import config as cfg
            return cfg

    # LOGIC
    def test_all_vars_present_loads_successfully(self):
        cfg = self._reload_config(self._REQUIRED_VARS)
        self.assertEqual(cfg.S3_BUCKET, "test-bucket")
        self.assertEqual(cfg.S3_REPORTS_PREFIX, "reports/")
        self.assertIsNotNone(cfg.TIMEZONE)

    def test_missing_s3_bucket_raises(self):
        env = {k: v for k, v in self._REQUIRED_VARS.items() if k != "S3_BUCKET"}
        if "config" in sys.modules:
            del sys.modules["config"]
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(EnvironmentError) as ctx:
                import config  # noqa: F401
            self.assertIn("S3_BUCKET", str(ctx.exception))

    def test_missing_db_secret_id_raises(self):
        env = {k: v for k, v in self._REQUIRED_VARS.items() if k != "DB_SECRET_ID"}
        if "config" in sys.modules:
            del sys.modules["config"]
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(EnvironmentError) as ctx:
                import config  # noqa: F401
            self.assertIn("DB_SECRET_ID", str(ctx.exception))

    def test_timezone_is_toronto(self):
        cfg = self._reload_config(self._REQUIRED_VARS)
        self.assertEqual(str(cfg.TIMEZONE), "America/Toronto")

    def tearDown(self):
        if "config" in sys.modules:
            del sys.modules["config"]


if __name__ == "__main__":
    unittest.main()