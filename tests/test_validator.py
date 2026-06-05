# BOILERPLATE
import sys
import os
import unittest
from unittest.mock import patch

import pandas as pd


def _make_valid_row(**overrides):
    row = {
        "trade_id": "T001",
        "desk_code": "EQTY",
        "trade_date": "2024-01-15",
        "instrument_type": "SWAP",
        "notional_amount": "1000000",
        "currency": "USD",
        "counterparty_id": "CP01",
    }
    row.update(overrides)
    return row


def _make_df(*rows):
    return pd.DataFrame(list(rows)).astype(str)


class TestValidator(unittest.TestCase):

    def setUp(self):
        if "config" in sys.modules:
            del sys.modules["config"]
        env = {
            "S3_BUCKET": "b", "S3_INPUT_PREFIX": "i/", "S3_REPORTS_PREFIX": "r/",
            "S3_ERRORS_PREFIX": "e/", "DB_SECRET_ID": "sid",
            "SNS_TOPIC_ARN_SUCCESS": "arn:s", "SNS_TOPIC_ARN_FAILURE": "arn:f",
            "AWS_REGION": "us-east-1",
        }
        self._env_patch = patch.dict(os.environ, env, clear=True)
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()
        for mod in ["config", "validator"]:
            if mod in sys.modules:
                del sys.modules[mod]

    # LOGIC — all valid rows pass
    def test_all_valid_rows_pass(self):
        df = _make_df(_make_valid_row(), _make_valid_row(trade_id="T002"))
        import validator
        valid_df, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(rejected_df), 0)

    # LOGIC — missing required field
    def test_missing_trade_id_rejected(self):
        df = _make_df(_make_valid_row(trade_id=""))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Missing required field: trade_id", rejected_df.iloc[0]["rejection_reason"])

    def test_missing_notional_rejected(self):
        df = _make_df(_make_valid_row(notional_amount="   "))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Missing required field: notional_amount", rejected_df.iloc[0]["rejection_reason"])

    # LOGIC — invalid trade_date format
    def test_invalid_trade_date_format_rejected(self):
        df = _make_df(_make_valid_row(trade_date="15-01-2024"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "15-01-2024")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Invalid trade_date format", rejected_df.iloc[0]["rejection_reason"])

    # LOGIC — non-numeric notional
    def test_non_numeric_notional_rejected(self):
        df = _make_df(_make_valid_row(notional_amount="abc"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Invalid notional_amount: not numeric", rejected_df.iloc[0]["rejection_reason"])

    # LOGIC — non-positive notional
    def test_zero_notional_rejected(self):
        df = _make_df(_make_valid_row(notional_amount="0"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Invalid notional_amount: must be positive", rejected_df.iloc[0]["rejection_reason"])

    def test_negative_notional_rejected(self):
        df = _make_df(_make_valid_row(notional_amount="-500"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Invalid notional_amount: must be positive", rejected_df.iloc[0]["rejection_reason"])

    # LOGIC — desk_code mismatch
    def test_desk_code_mismatch_rejected(self):
        df = _make_df(_make_valid_row(desk_code="FIXED"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        reason = rejected_df.iloc[0]["rejection_reason"]
        self.assertIn("desk_code mismatch", reason)
        self.assertIn("EQTY", reason)
        self.assertIn("FIXED", reason)

    # LOGIC — trade_date mismatch
    def test_trade_date_mismatch_rejected(self):
        df = _make_df(_make_valid_row(trade_date="2024-01-16"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(rejected_df), 1)
        reason = rejected_df.iloc[0]["rejection_reason"]
        self.assertIn("trade_date mismatch", reason)
        self.assertIn("2024-01-15", reason)
        self.assertIn("2024-01-16", reason)

    # LOGIC — intra-file duplicate trade_id
    def test_duplicate_trade_id_second_row_rejected(self):
        df = _make_df(_make_valid_row(trade_id="T001"), _make_valid_row(trade_id="T001"))
        import validator
        valid_df, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("Duplicate trade_id within file", rejected_df.iloc[0]["rejection_reason"])

    # LOGIC — first-failing-check priority: missing field wins over date format
    def test_first_check_wins(self):
        df = _make_df(_make_valid_row(trade_id="", trade_date="bad-date"))
        import validator
        _, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        reason = rejected_df.iloc[0]["rejection_reason"]
        self.assertIn("Missing required field: trade_id", reason)

    # LOGIC — valid_df has correct final types
    def test_valid_df_types(self):
        from datetime import date
        df = _make_df(_make_valid_row())
        import validator
        valid_df, _ = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(valid_df.iloc[0]["trade_date"], date(2024, 1, 15))
        self.assertIsInstance(valid_df.iloc[0]["notional_amount"], float)
        self.assertIsInstance(valid_df.iloc[0]["trade_id"], str)

    # LOGIC — mixed valid and invalid
    def test_mixed_rows(self):
        rows = [
            _make_valid_row(trade_id="T001"),
            _make_valid_row(trade_id="T002", notional_amount="abc"),
            _make_valid_row(trade_id="T003"),
        ]
        df = _make_df(*rows)
        import validator
        valid_df, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(rejected_df), 1)

    def test_empty_dataframe_returns_empty_both(self):
        df = pd.DataFrame(columns=["trade_id", "desk_code", "trade_date", "instrument_type", "notional_amount", "currency", "counterparty_id"])
        import validator
        valid_df, rejected_df = validator.validate_rows(df, "EQTY", "2024-01-15")
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 0)


if __name__ == "__main__":
    unittest.main()