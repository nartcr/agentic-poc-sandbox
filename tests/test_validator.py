# BOILERPLATE
import math
import unittest

import pandas as pd

from validator import validate_rows

# LOGIC — helpers to build test DataFrames


def _base_row(**overrides) -> dict:
    """Return a valid row dict, optionally overriding fields."""
    row = {
        "trade_id": "TRD-001",
        "desk_code": "EQTY",
        "instrument_type": "EQUITY_SWAP",
        "notional_amount": "1000000.00",
        "currency": "USD",
        "counterparty_id": "CP-123",
        "trade_date": "2026-06-15",
    }
    row.update(overrides)
    return row


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


class TestValidateRowsAllValid(unittest.TestCase):
    # LOGIC — TAC-1: all valid rows produce empty rejected_df

    def test_all_valid_rows_accepted(self):
        rows = [_base_row(trade_id=f"TRD-{i:04d}") for i in range(1000)]
        df = _df(*rows)
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1000)
        self.assertEqual(len(rejected_df), 0)

    def test_valid_df_preserves_extra_columns(self):
        row = _base_row()
        row["extra_col"] = "extra_value"
        df = _df(row)
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1)
        self.assertIn("extra_col", valid_df.columns)

    def test_rejected_df_has_rejection_reason_column(self):
        # Even with zero rejections the schema has the column
        df = _df(_base_row())
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertIn("rejection_reason", rejected_df.columns)


class TestRule1MissingRequiredFields(unittest.TestCase):
    # LOGIC — Rule 1: missing/null/empty required fields

    def test_null_trade_id_rejected(self):
        df = _df(_base_row(trade_id=None))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("missing_required_field", rejected_df.iloc[0]["rejection_reason"])
        self.assertIn("trade_id", rejected_df.iloc[0]["rejection_reason"])

    def test_null_counterparty_id_rejected(self):
        df = _df(_base_row(counterparty_id=None))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("counterparty_id", rejected_df.iloc[0]["rejection_reason"])

    def test_empty_string_instrument_type_rejected(self):
        df = _df(_base_row(instrument_type=""))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("instrument_type", rejected_df.iloc[0]["rejection_reason"])

    def test_whitespace_only_field_rejected(self):
        df = _df(_base_row(currency="   "))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        # whitespace-only currency is blank (rule 1) before reaching rule 5
        self.assertIn("missing_required_field", rejected_df.iloc[0]["rejection_reason"])

    def test_nan_notional_rejected_as_missing(self):
        df = _df(_base_row(notional_amount=float("nan")))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("missing_required_field", rejected_df.iloc[0]["rejection_reason"])


class TestRule2TradeDateValidation(unittest.TestCase):
    # LOGIC — Rule 2: trade_date parses as YYYY-MM-DD and matches filename date

    def test_invalid_date_format_rejected(self):
        df = _df(_base_row(trade_date="15-06-2026"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_trade_date_format", rejected_df.iloc[0]["rejection_reason"])

    def test_date_mismatch_rejected(self):
        df = _df(_base_row(trade_date="2026-06-16"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("trade_date_mismatch", rejected_df.iloc[0]["rejection_reason"])

    def test_matching_date_passes(self):
        df = _df(_base_row(trade_date="2026-06-15"))
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(rejected_df), 0)

    def test_non_existent_date_rejected(self):
        df = _df(_base_row(trade_date="2026-02-30"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-02-30")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_trade_date_format", rejected_df.iloc[0]["rejection_reason"])


class TestRule3DeskCodeValidation(unittest.TestCase):
    # LOGIC — Rule 3: desk_code matches filename desk code

    def test_wrong_desk_code_rejected(self):
        df = _df(_base_row(desk_code="FICC"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("desk_code_mismatch", rejected_df.iloc[0]["rejection_reason"])
        self.assertIn("FICC", rejected_df.iloc[0]["rejection_reason"])
        self.assertIn("EQTY", rejected_df.iloc[0]["rejection_reason"])

    def test_matching_desk_code_passes(self):
        df = _df(_base_row(desk_code="EQTY"))
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(rejected_df), 0)


class TestRule4NotionalAmount(unittest.TestCase):
    # LOGIC — Rule 4: notional_amount is a valid finite decimal

    def test_non_numeric_notional_rejected(self):
        df = _df(_base_row(notional_amount="abc"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_notional_amount", rejected_df.iloc[0]["rejection_reason"])
        self.assertIn("'abc'", rejected_df.iloc[0]["rejection_reason"])

    def test_infinite_notional_rejected(self):
        df = _df(_base_row(notional_amount=float("inf")))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_notional_amount", rejected_df.iloc[0]["rejection_reason"])

    def test_negative_inf_notional_rejected(self):
        df = _df(_base_row(notional_amount=float("-inf")))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_notional_amount", rejected_df.iloc[0]["rejection_reason"])

    def test_zero_notional_is_valid(self):
        df = _df(_base_row(notional_amount="0.00"))
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(rejected_df), 0)

    def test_negative_notional_is_valid(self):
        df = _df(_base_row(notional_amount="-500000.50"))
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 1)
        self.assertEqual(len(rejected_df), 0)


class TestRule5Currency(unittest.TestCase):
    # LOGIC — Rule 5: currency is exactly 3 uppercase alphabetic characters

    def test_lowercase_currency_rejected(self):
        df = _df(_base_row(currency="usd"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_currency", rejected_df.iloc[0]["rejection_reason"])

    def test_two_char_currency_rejected(self):
        df = _df(_base_row(currency="US"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_currency", rejected_df.iloc[0]["rejection_reason"])

    def test_four_char_currency_rejected(self):
        df = _df(_base_row(currency="USDX"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_currency", rejected_df.iloc[0]["rejection_reason"])

    def test_currency_with_digit_rejected(self):
        df = _df(_base_row(currency="U5D"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_currency", rejected_df.iloc[0]["rejection_reason"])

    def test_valid_three_char_uppercase_currency_accepted(self):
        for code in ["USD", "EUR", "GBP", "CAD", "JPY"]:
            with self.subTest(currency=code):
                df = _df(_base_row(currency=code))
                valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
                self.assertEqual(len(valid_df), 1, f"Expected {code} to be valid")
                self.assertEqual(len(rejected_df), 0)


class TestRule6TradeIdNonEmpty(unittest.TestCase):
    # LOGIC — Rule 6: trade_id is non-empty string
    # (empty string that might survive NaN check in pandas)

    def test_empty_string_trade_id_rejected(self):
        # Pandas won't treat "" as NaN so rule 1 passes, rule 6 catches it
        row = _base_row(trade_id="")
        df = _df(row)
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        # Rule 1 catches empty string as missing_required_field
        self.assertTrue(
            "missing_required_field" in rejected_df.iloc[0]["rejection_reason"]
            or "empty_trade_id" in rejected_df.iloc[0]["rejection_reason"]
        )


class TestFirstFailureWins(unittest.TestCase):
    # LOGIC — first failing rule wins; subsequent rules not evaluated

    def test_rule1_wins_over_rule2(self):
        # Row has null counterparty_id (rule 1) AND wrong trade_date (rule 2)
        df = _df(_base_row(counterparty_id=None, trade_date="2026-06-16"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("missing_required_field", rejected_df.iloc[0]["rejection_reason"])
        self.assertIn("counterparty_id", rejected_df.iloc[0]["rejection_reason"])

    def test_rule2_wins_over_rule3(self):
        # Row has bad date format (rule 2) AND wrong desk_code (rule 3)
        df = _df(_base_row(trade_date="bad-date", desk_code="FICC"))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(rejected_df), 1)
        self.assertIn("invalid_trade_date_format", rejected_df.iloc[0]["rejection_reason"])


class TestTAC2FiveRejections(unittest.TestCase):
    # LOGIC — TAC-2: 10 rows with exactly 5 invalid (one per distinct rule category)

    def test_exactly_five_rejections_one_per_rule(self):
        rows = [
            # 5 valid rows
            _base_row(trade_id="TRD-001"),
            _base_row(trade_id="TRD-002"),
            _base_row(trade_id="TRD-003"),
            _base_row(trade_id="TRD-004"),
            _base_row(trade_id="TRD-005"),
            # Rule 1 violation: missing required field
            _base_row(trade_id="TRD-006", counterparty_id=None),
            # Rule 2 violation: invalid date format
            _base_row(trade_id="TRD-007", trade_date="06/15/2026"),
            # Rule 3 violation: desk_code mismatch
            _base_row(trade_id="TRD-008", desk_code="FICC"),
            # Rule 4 violation: non-numeric notional
            _base_row(trade_id="TRD-009", notional_amount="N/A"),
            # Rule 5 violation: invalid currency
            _base_row(trade_id="TRD-010", currency="xx"),
        ]
        df = _df(*rows)
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")

        self.assertEqual(len(valid_df), 5)
        self.assertEqual(len(rejected_df), 5)

        # All rejected rows have a non-empty rejection_reason
        for _, row in rejected_df.iterrows():
            self.assertTrue(len(str(row["rejection_reason"]).strip()) > 0)

        # Verify each expected reason appears exactly once
        reasons = list(rejected_df["rejection_reason"])
        self.assertTrue(any("missing_required_field" in r for r in reasons))
        self.assertTrue(any("invalid_trade_date_format" in r for r in reasons))
        self.assertTrue(any("desk_code_mismatch" in r for r in reasons))
        self.assertTrue(any("invalid_notional_amount" in r for r in reasons))
        self.assertTrue(any("invalid_currency" in r for r in reasons))


class TestRejectedDfSchema(unittest.TestCase):
    # LOGIC — rejected_df preserves original columns and appends rejection_reason last

    def test_rejected_df_column_order(self):
        df = _df(_base_row(counterparty_id=None))
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        cols = list(rejected_df.columns)
        self.assertEqual(cols[-1], "rejection_reason")
        # All original columns present
        for col in df.columns:
            self.assertIn(col, cols)

    def test_rejected_df_extra_columns_preserved(self):
        row = _base_row(counterparty_id=None)
        row["source_system"] = "LEGACY"
        df = _df(row)
        _, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertIn("source_system", rejected_df.columns)
        self.assertEqual(rejected_df.iloc[0]["source_system"], "LEGACY")


class TestEmptyDataFrame(unittest.TestCase):
    # LOGIC — empty input produces empty outputs with correct schemas

    def test_empty_df_returns_empty_valid_and_rejected(self):
        df = pd.DataFrame(
            columns=[
                "trade_id", "desk_code", "instrument_type",
                "notional_amount", "currency", "counterparty_id", "trade_date",
            ]
        )
        valid_df, rejected_df = validate_rows(df, "EQTY", "2026-06-15")
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 0)
        self.assertIn("rejection_reason", rejected_df.columns)