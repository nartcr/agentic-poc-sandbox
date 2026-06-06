# BOILERPLATE
import sys
import os
import unittest
import pandas as pd

# BOILERPLATE — ensure src/ is importable when tests are run from repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.validator import validate_rows  # BOILERPLATE


class TestValidateRowsAllValid(unittest.TestCase):
    """TAC-1 / BAC-2: All valid rows produce an empty rejected_df."""

    def _make_valid_row(self, trade_id="TRD-001"):
        # LOGIC — minimal conformant row against all seven mandatory fields
        return {
            "trade_id": trade_id,
            "desk_code": "EQTY",
            "trade_date": "2026-06-15",
            "instrument_type": "EQUITY_SWAP",
            "notional_amount": "1000000.00",
            "currency": "USD",
            "counterparty_id": "CP-999",
        }

    def test_all_valid_rows_produces_empty_rejected_df(self):
        # LOGIC
        rows = [self._make_valid_row(trade_id=f"TRD-{i:03d}") for i in range(5)]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 0)
        self.assertEqual(len(valid_df), 5)

    def test_valid_df_notional_amount_cast_to_float64(self):
        # LOGIC — valid_df must have notional_amount as float64, not string
        rows = [self._make_valid_row()]
        df = pd.DataFrame(rows)
        valid_df, _ = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(valid_df["notional_amount"].dtype, "float64")


class TestValidateRowsEmptyTradeId(unittest.TestCase):
    """TAC-2 / BAC-2: Row with empty trade_id lands in rejected_df with reason containing 'trade_id'."""

    def test_empty_trade_id_rejected(self):
        # LOGIC
        rows = [
            {
                "trade_id": "",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "500000.00",
                "currency": "EUR",
                "counterparty_id": "CP-001",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(len(valid_df), 0)
        self.assertIn("trade_id", rejected_df.iloc[0]["rejection_reason"])

    def test_whitespace_only_trade_id_rejected(self):
        # LOGIC — whitespace-only strings should be treated as empty after strip
        rows = [
            {
                "trade_id": "   ",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "500000.00",
                "currency": "EUR",
                "counterparty_id": "CP-001",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertIn("trade_id", rejected_df.iloc[0]["rejection_reason"])


class TestValidateRowsNonNumericNotional(unittest.TestCase):
    """TAC-2 / BAC-2: Row with non-numeric notional_amount is rejected with reason containing 'notional_amount'."""

    def test_non_numeric_notional_rejected(self):
        # LOGIC
        rows = [
            {
                "trade_id": "TRD-100",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "not_a_number",
                "currency": "USD",
                "counterparty_id": "CP-002",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(len(valid_df), 0)
        self.assertIn("notional_amount", rejected_df.iloc[0]["rejection_reason"])

    def test_empty_notional_rejected(self):
        # LOGIC — empty string is not a valid float
        rows = [
            {
                "trade_id": "TRD-101",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "",
                "currency": "USD",
                "counterparty_id": "CP-002",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        # LOGIC — empty mandatory field AND non-numeric both touch notional_amount
        self.assertIn("notional_amount", rejected_df.iloc[0]["rejection_reason"])

    def test_inf_notional_rejected(self):
        # LOGIC — infinite float values are not allowed per design (must be finite)
        rows = [
            {
                "trade_id": "TRD-102",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "inf",
                "currency": "USD",
                "counterparty_id": "CP-002",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertIn("notional_amount", rejected_df.iloc[0]["rejection_reason"])


class TestValidateRowsInvalidCurrency(unittest.TestCase):
    """TAC-2 / BAC-2: Row with invalid currency code is rejected with reason containing 'currency'."""

    def test_currency_with_digit_rejected(self):
        # LOGIC — "USD1" does not match ^[A-Z]{3}$
        rows = [
            {
                "trade_id": "TRD-200",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "250000.00",
                "currency": "USD1",
                "counterparty_id": "CP-003",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(len(valid_df), 0)
        self.assertIn("currency", rejected_df.iloc[0]["rejection_reason"])

    def test_lowercase_currency_rejected(self):
        # LOGIC — "usd" does not match ^[A-Z]{3}$ (lowercase)
        rows = [
            {
                "trade_id": "TRD-201",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "250000.00",
                "currency": "usd",
                "counterparty_id": "CP-003",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertIn("currency", rejected_df.iloc[0]["rejection_reason"])

    def test_two_char_currency_rejected(self):
        # LOGIC — "US" is only 2 characters, does not match ^[A-Z]{3}$
        rows = [
            {
                "trade_id": "TRD-202",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "250000.00",
                "currency": "US",
                "counterparty_id": "CP-003",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertIn("currency", rejected_df.iloc[0]["rejection_reason"])


class TestValidateRowsDeskCodeMismatch(unittest.TestCase):
    """TAC-2 / BAC-2: Row with desk_code not matching filename-derived desk_code is rejected."""

    def test_desk_code_mismatch_rejected(self):
        # LOGIC — row says "RATES" but filename-derived desk_code is "EQTY"
        rows = [
            {
                "trade_id": "TRD-300",
                "desk_code": "RATES",
                "trade_date": "2026-06-15",
                "instrument_type": "INTEREST_RATE_SWAP",
                "notional_amount": "1000000.00",
                "currency": "USD",
                "counterparty_id": "CP-004",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(len(valid_df), 0)
        self.assertIn("desk_code", rejected_df.iloc[0]["rejection_reason"])

    def test_desk_code_match_accepted(self):
        # LOGIC — row desk_code matches filename-derived desk_code → not rejected for this reason
        rows = [
            {
                "trade_id": "TRD-301",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "750000.00",
                "currency": "GBP",
                "counterparty_id": "CP-005",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 0)
        self.assertEqual(len(valid_df), 1)


class TestValidateRowsMultipleFailures(unittest.TestCase):
    """TAC-2 / BAC-2: Row with multiple failures has all failing field names in rejection_reason."""

    def test_multiple_failures_all_reasons_present(self):
        # LOGIC — trade_id empty, notional_amount invalid, currency invalid
        rows = [
            {
                "trade_id": "",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "NOTANUMBER",
                "currency": "X1",
                "counterparty_id": "CP-006",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(len(valid_df), 0)

        reason = rejected_df.iloc[0]["rejection_reason"]
        # LOGIC — all three failing fields must be mentioned in the pipe-delimited reason string
        self.assertIn("trade_id", reason)
        self.assertIn("notional_amount", reason)
        self.assertIn("currency", reason)

    def test_multiple_failures_pipe_delimiter_present(self):
        # LOGIC — when more than one field fails, the delimiter "|" must appear in rejection_reason
        rows = [
            {
                "trade_id": "",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "BAD",
                "currency": "USD",
                "counterparty_id": "CP-007",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        reason = rejected_df.iloc[0]["rejection_reason"]
        # LOGIC — at least two failures (trade_id + notional_amount) → pipe must separate them
        self.assertIn("|", reason)

    def test_rejected_df_has_rejection_reason_column(self):
        # LOGIC — rejected_df must carry the rejection_reason column regardless of which field fails
        rows = [
            {
                "trade_id": "TRD-400",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "BADINPUT",
                "currency": "USD",
                "counterparty_id": "CP-008",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertIn("rejection_reason", rejected_df.columns)

    def test_valid_rows_do_not_have_rejection_reason_column(self):
        # LOGIC — valid_df must NOT carry a rejection_reason column
        rows = [
            {
                "trade_id": "TRD-500",
                "desk_code": "EQTY",
                "trade_date": "2026-06-15",
                "instrument_type": "EQUITY_SWAP",
                "notional_amount": "100000.00",
                "currency": "CAD",
                "counterparty_id": "CP-009",
            }
        ]
        df = pd.DataFrame(rows)
        valid_df, _ = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertNotIn("rejection_reason", valid_df.columns)

    def test_desk_code_and_currency_both_fail(self):
        # LOGIC — desk_code mismatch + bad currency in same row
        rows = [
            {
                "trade_id": "TRD-600",
                "desk_code": "RATES",
                "trade_date": "2026-06-15",
                "instrument_type": "IRS",
                "notional_amount": "500000.00",
                "currency": "!!",
                "counterparty_id": "CP-010",
            }
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(rejected_df), 1)
        reason = rejected_df.iloc[0]["rejection_reason"]
        self.assertIn("desk_code", reason)
        self.assertIn("currency", reason)


class TestValidateRowsMixedBatch(unittest.TestCase):
    """Integration-style: mixed valid and invalid rows are split correctly."""

    def test_mixed_batch_split(self):
        # LOGIC — 3 valid, 2 invalid → valid_df has 3 rows, rejected_df has 2
        rows = [
            # valid
            {"trade_id": "T001", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "100.00", "currency": "USD", "counterparty_id": "CP1"},
            {"trade_id": "T002", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "200.00", "currency": "EUR", "counterparty_id": "CP2"},
            {"trade_id": "T003", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "300.00", "currency": "GBP", "counterparty_id": "CP3"},
            # invalid — bad currency
            {"trade_id": "T004", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "400.00", "currency": "TOOLONG", "counterparty_id": "CP4"},
            # invalid — empty trade_id
            {"trade_id": "", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "500.00", "currency": "JPY", "counterparty_id": "CP5"},
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(valid_df), 3)
        self.assertEqual(len(rejected_df), 2)

    def test_all_invalid_produces_empty_valid_df(self):
        # LOGIC — every row has a bad notional_amount → all rejected, valid_df is empty
        rows = [
            {"trade_id": f"T{i}", "desk_code": "EQTY", "trade_date": "2026-06-15",
             "instrument_type": "EQ", "notional_amount": "BAD", "currency": "USD", "counterparty_id": "CP"}
            for i in range(3)
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df, desk_code="EQTY", trade_date="2026-06-15")

        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 3)


if __name__ == "__main__":
    unittest.main()