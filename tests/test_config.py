# BOILERPLATE
import os
import unittest
from unittest.mock import patch


class TestLoadConfig(unittest.TestCase):

    def _make_env(self, overrides: dict = None) -> dict:
        # BOILERPLATE — baseline environment with all required vars set
        base = {
            "S3_BUCKET": "my-trade-bucket",
            "S3_INPUT_PREFIX": "positions/",
            "S3_REPORTS_PREFIX": "reports/",
            "S3_ERRORS_PREFIX": "errors/",
            "DB_SECRET_ID": "prod/aurora/trades",
            "SNS_SUCCESS_TOPIC_ARN": "arn:aws:sns:ca-central-1:123456789012:trade-success",
            "SNS_FAILURE_TOPIC_ARN": "arn:aws:sns:ca-central-1:123456789012:trade-failure",
            "TZ": "America/Toronto",
        }
        if overrides:
            base.update(overrides)
        return base

    def test_all_required_vars_populated(self):
        # LOGIC — verify every field on Config is set from the corresponding env var
        env = self._make_env({"AUDIT_TABLE": "app.pipeline_audit"})
        with patch.dict(os.environ, env, clear=True):
            from config import load_config
            cfg = load_config()

        self.assertEqual(cfg.S3_BUCKET, "my-trade-bucket")
        self.assertEqual(cfg.S3_INPUT_PREFIX, "positions/")
        self.assertEqual(cfg.S3_REPORTS_PREFIX, "reports/")
        self.assertEqual(cfg.S3_ERRORS_PREFIX, "errors/")
        self.assertEqual(cfg.DB_SECRET_ID, "prod/aurora/trades")
        self.assertEqual(cfg.SNS_SUCCESS_TOPIC_ARN, "arn:aws:sns:ca-central-1:123456789012:trade-success")
        self.assertEqual(cfg.SNS_FAILURE_TOPIC_ARN, "arn:aws:sns:ca-central-1:123456789012:trade-failure")
        self.assertEqual(cfg.AUDIT_TABLE, "app.pipeline_audit")
        self.assertEqual(cfg.TZ, "America/Toronto")

    def test_audit_table_defaults_when_absent(self):
        # LOGIC — AUDIT_TABLE must default to "app.pipeline_audit" when not set
        env = self._make_env()
        env.pop("AUDIT_TABLE", None)
        with patch.dict(os.environ, env, clear=True):
            from config import load_config
            cfg = load_config()

        self.assertEqual(cfg.AUDIT_TABLE, "app.pipeline_audit")

    def test_missing_required_var_raises_key_error(self):
        # LOGIC — any missing required var must raise KeyError immediately
        required_vars = [
            "S3_BUCKET",
            "S3_INPUT_PREFIX",
            "S3_REPORTS_PREFIX",
            "S3_ERRORS_PREFIX",
            "DB_SECRET_ID",
            "SNS_SUCCESS_TOPIC_ARN",
            "SNS_FAILURE_TOPIC_ARN",
            "TZ",
        ]
        for var in required_vars:
            with self.subTest(missing_var=var):
                env = self._make_env()
                env.pop(var)
                with patch.dict(os.environ, env, clear=True):
                    from config import load_config
                    with self.assertRaises(KeyError):
                        load_config()

    def test_config_is_frozen(self):
        # LOGIC — Config must be immutable (frozen dataclass); mutation must raise
        env = self._make_env()
        with patch.dict(os.environ, env, clear=True):
            from config import load_config
            cfg = load_config()

        with self.assertRaises((AttributeError, TypeError)):
            cfg.S3_BUCKET = "something-else"  # type: ignore[misc]

    def test_reports_prefix_value(self):
        # LOGIC — reports prefix must be exactly "reports/" as required by design
        env = self._make_env({"S3_REPORTS_PREFIX": "reports/"})
        with patch.dict(os.environ, env, clear=True):
            from config import load_config
            cfg = load_config()

        self.assertEqual(cfg.S3_REPORTS_PREFIX, "reports/")


class TestConfigDataclass(unittest.TestCase):

    def test_config_fields_are_all_str(self):
        # LOGIC — all Config fields must be plain strings (no int/bool coercion)
        env = {
            "S3_BUCKET": "bucket",
            "S3_INPUT_PREFIX": "positions/",
            "S3_REPORTS_PREFIX": "reports/",
            "S3_ERRORS_PREFIX": "errors/",
            "DB_SECRET_ID": "secret-id",
            "SNS_SUCCESS_TOPIC_ARN": "arn:aws:sns:x",
            "SNS_FAILURE_TOPIC_ARN": "arn:aws:sns:y",
            "AUDIT_TABLE": "app.pipeline_audit",
            "TZ": "America/Toronto",
        }
        with patch.dict(os.environ, env, clear=True):
            from config import load_config
            cfg = load_config()

        for field_name in [
            "S3_BUCKET", "S3_INPUT_PREFIX", "S3_REPORTS_PREFIX", "S3_ERRORS_PREFIX",
            "DB_SECRET_ID", "SNS_SUCCESS_TOPIC_ARN", "SNS_FAILURE_TOPIC_ARN",
            "AUDIT_TABLE", "TZ",
        ]:
            with self.subTest(field=field_name):
                self.assertIsInstance(getattr(cfg, field_name), str)


if __name__ == "__main__":
    unittest.main()