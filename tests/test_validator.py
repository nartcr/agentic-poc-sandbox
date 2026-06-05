# BOILERPLATE
import datetime
import unittest

import pandas as pd

from validator import validate_rows


def _make_valid_row(**overrides) -> dict:
    # BOILERPLATE — produce a single well-formed row dict; caller overrides specific fields
    row = {
        "trade_id":        "T001",
        "desk_code":       "EQTY",
        "trade_date":      "2026-06-15",
        "instrument_type": "SWAP",
        "notional_amount": "1000000.00",
        "currency":        "USD",
        "counterparty_id": "CP001",
        "_source_row":     1,
    }
    row.update(overrides)
    return row


def _df_from_rows(*rows) -> pd.DataFrame:
    # BOILERPLATE — build a DataFrame from dicts; all object columns start as str
    return pd.DataFrame(list(rows)).astype(
        {c: str for c in pd.DataFrame(list(rows)).select_dtypes(include="object").columns}
    )


class TestValidateRowsAllValid(unittest.TestCase):

    def test_fully_valid_file_produces_zero_rejections(self):
        # LOGIC — TAC-1: 1000 clean rows must produce 0 rejections
        rows = [_make_valid_row(trade_id=f"T{i:04d}", _source_row=i) for i in range(1, 1001)]
        df = _df_from_rows(*rows)
        valid_df, rejected_df = validate_rows(df)

        self.assertEqual(len(valid_df), 1000)
        self.assertEqual(len(rejected_df), 0)

    def test_valid_rows_have_notional_as_float64(self):
        # LOGIC — valid_df must cast notional_amount to float64
        df = _df_from_rows(_make_valid_row(notional_amount="12345.67"))
        valid_df, _ = validate_rows(df)

        self.assertEqual(valid_df["notional_amount"].dtype, "float64")
        self.assertAlmostEqual(valid_df["notional_amount"].iloc[0], 12345.67)

    def test_valid_rows_have_trade_date_as_date(self):
        # LOGIC — valid_df must cast trade_date to datetime.date
        df = _df_from_rows(_make_valid_row(trade_date="2026-06-15"))
        valid_df, _ = validate_rows(df)

        self.assertIsInstance(valid_df["trade_date"].iloc[0], datetime.date)
        self.assertEqual(valid_df["trade_date"].iloc[0], datetime.date(2026, 6, 15))

    def test_source_row_preserved_in_valid_df(self):
        # LOGIC — _source_row must pass through unchanged
        df = _df_from_rows(_make_valid_row(_source_row=42))
        valid_df, _ = validate_rows(df)

        self.assertEqual(int(valid_df["_source_row"].iloc[0]), 42)


class TestValidateRowsRejections(unittest.TestCase):

    def test_missing_trade_id_rejected(self):
        # LOGIC — Rule 1
        df = _df_from_rows(
            _make_valid_row(trade_id=""),
            _make_valid_row(trade_id="T002", _source_row=2),
        )
        _, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "trade_id is missing or empty")

    def test_missing_desk_code_rejected(self):
        # LOGIC — Rule 2
        df = _df_from_rows(_make_valid_row(desk_code=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "desk_code is missing or empty")

    def test_missing_trade_date_rejected(self):
        # LOGIC — Rule 3
        df = _df_from_rows(_make_valid_row(trade_date=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "trade_date is missing or empty")

    def test_invalid_trade_date_format_rejected(self):
        # LOGIC — Rule 4: present but not YYYY-MM-DD
        df = _df_from_rows(_make_valid_row(trade_date="15/06/2026"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )

    def test_missing_instrument_type_rejected(self):
        # LOGIC — Rule 5
        df = _df_from_rows(_make_valid_row(instrument_type=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "instrument_type is missing or empty"
        )

    def test_missing_notional_amount_rejected(self):
        # LOGIC — Rule 6
        df = _df_from_rows(_make_valid_row(notional_amount=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "notional_amount is missing or empty"
        )

    def test_non_numeric_notional_amount_rejected(self):
        # LOGIC — Rule 7
        df = _df_from_rows(_make_valid_row(notional_amount="not_a_number"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "notional_amount is not numeric"
        )

    def test_missing_currency_rejected(self):
        # LOGIC — Rule 8
        df = _df_from_rows(_make_valid_row(currency=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "currency is missing or empty")

    def test_currency_lowercase_rejected(self):
        # LOGIC — Rule 9: lowercase letters fail the 3-uppercase-letter check
        df = _df_from_rows(_make_valid_row(currency="usd"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "currency must be a 3-letter ISO code"
        )

    def test_currency_wrong_length_rejected(self):
        # LOGIC — Rule 9: must be exactly 3 characters
        df = _df_from_rows(_make_valid_row(currency="USDD"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "currency must be a 3-letter ISO code"
        )

    def test_currency_two_letters_rejected(self):
        # LOGIC — Rule 9: 2-letter code is invalid
        df = _df_from_rows(_make_valid_row(currency="US"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "currency must be a 3-letter ISO code"
        )

    def test_missing_counterparty_id_rejected(self):
        # LOGIC — Rule 10
        df = _df_from_rows(_make_valid_row(counterparty_id=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "counterparty_id is missing or empty"
        )

    def test_first_failing_rule_wins_trade_id_and_desk_code_both_missing(self):
        # LOGIC — first-failing-rule wins: trade_id missing beats desk_code missing
        df = _df_from_rows(_make_valid_row(trade_id="", desk_code=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "trade_id is missing or empty")

    def test_first_failing_rule_wins_trade_date_missing_beats_format(self):
        # LOGIC — Rule 3 (missing) fires before Rule 4 (bad format) for empty trade_date
        df = _df_from_rows(_make_valid_row(trade_date=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "trade_date is missing or empty")

    def test_first_failing_rule_wins_notional_missing_beats_non_numeric(self):
        # LOGIC — Rule 6 (missing) fires before Rule 7 (non-numeric) for empty notional_amount
        df = _df_from_rows(_make_valid_row(notional_amount=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "notional_amount is missing or empty"
        )

    def test_first_failing_rule_wins_currency_missing_beats_format(self):
        # LOGIC — Rule 8 (missing) fires before Rule 9 (bad format) for empty currency
        df = _df_from_rows(_make_valid_row(currency=""))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "currency is missing or empty")


class TestValidateRowsMixedBatch(unittest.TestCase):

    def test_five_distinct_violations_tac2(self):
        # LOGIC — TAC-2: exactly 5 rows with 5 distinct violation types
        rows = [
            _make_valid_row(trade_id="",             _source_row=1),   # Rule 1
            _make_valid_row(trade_date="bad-date",   _source_row=2),   # Rule 4
            _make_valid_row(notional_amount="abc",   _source_row=3),   # Rule 7
            _make_valid_row(currency="XXXX",         _source_row=4),   # Rule 9
            _make_valid_row(counterparty_id="",      _source_row=5),   # Rule 10
        ]
        df = _df_from_rows(*rows)
        _, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 5)

        reasons = rejected_df["rejection_reason"].tolist()
        self.assertIn("trade_id is missing or empty", reasons)
        self.assertIn("trade_date is not a valid date (expected YYYY-MM-DD)", reasons)
        self.assertIn("notional_amount is not numeric", reasons)
        self.assertIn("currency must be a 3-letter ISO code", reasons)
        self.assertIn("counterparty_id is missing or empty", reasons)

    def test_five_distinct_violations_source_rows_preserved(self):
        # LOGIC — _source_row in rejected_df must match original assignment
        rows = [
            _make_valid_row(trade_id="",           _source_row=10),
            _make_valid_row(trade_date="bad",      _source_row=20),
            _make_valid_row(notional_amount="abc", _source_row=30),
            _make_valid_row(currency="XX",         _source_row=40),
            _make_valid_row(counterparty_id="",    _source_row=50),
        ]
        df = _df_from_rows(*rows)
        _, rejected_df = validate_rows(df)

        source_rows = set(int(r) for r in rejected_df["_source_row"].tolist())
        self.assertEqual(source_rows, {10, 20, 30, 40, 50})

    def test_mixed_valid_and_invalid_split_correctly(self):
        # LOGIC — valid rows must not appear in rejected_df and vice versa
        rows = [
            _make_valid_row(trade_id="T001", _source_row=1),               # valid
            _make_valid_row(trade_id="",     _source_row=2),               # invalid
            _make_valid_row(trade_id="T003", _source_row=3),               # valid
        ]
        df = _df_from_rows(*rows)
        valid_df, rejected_df = validate_rows(df)

        self.assertEqual(len(valid_df), 2)
        self.assertEqual(len(rejected_df), 1)

        valid_ids = set(valid_df["trade_id"].tolist())
        self.assertIn("T001", valid_ids)
        self.assertIn("T003", valid_ids)
        self.assertNotIn("", valid_ids)

    def test_rejection_reason_column_absent_from_valid_df(self):
        # LOGIC — rejection_reason must not appear in valid_df
        df = _df_from_rows(_make_valid_row())
        valid_df, _ = validate_rows(df)

        self.assertNotIn("rejection_reason", valid_df.columns)

    def test_rejection_reason_column_present_in_rejected_df(self):
        # LOGIC — rejection_reason must appear in rejected_df
        df = _df_from_rows(_make_valid_row(trade_id=""))
        _, rejected_df = validate_rows(df)

        self.assertIn("rejection_reason", rejected_df.columns)
        self.assertTrue(all(r != "" for r in rejected_df["rejection_reason"].tolist()))


class TestValidateRowsEdgeCases(unittest.TestCase):

    def test_whitespace_only_trade_id_treated_as_missing(self):
        # LOGIC — a field containing only spaces must be treated as empty
        df = _df_from_rows(_make_valid_row(trade_id="   "))
        _, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "trade_id is missing or empty")

    def test_whitespace_only_currency_treated_as_missing(self):
        # LOGIC — whitespace currency triggers missing rule (Rule 8) not format rule (Rule 9)
        df = _df_from_rows(_make_valid_row(currency="   "))
        _, rejected_df = validate_rows(df)

        self.assertEqual(rejected_df["rejection_reason"].iloc[0], "currency is missing or empty")

    def test_notional_amount_with_leading_zeros_valid(self):
        # LOGIC — "007.00" is numeric and must not be rejected
        df = _df_from_rows(_make_valid_row(notional_amount="007.00"))
        valid_df, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 0)
        self.assertAlmostEqual(valid_df["notional_amount"].iloc[0], 7.0)

    def test_negative_notional_amount_is_valid_numeric(self):
        # LOGIC — negative values are numeric; no rule rejects them
        df = _df_from_rows(_make_valid_row(notional_amount="-500.00"))
        valid_df, rejected_df = validate_rows(df)

        self.assertEqual(len(rejected_df), 0)
        self.assertAlmostEqual(valid_df["notional_amount"].iloc[0], -500.0)

    def test_currency_with_digits_rejected(self):
        # LOGIC — "U1D" contains a digit; fails regex ^[A-Z]{3}$
        df = _df_from_rows(_make_valid_row(currency="U1D"))
        _, rejected_df = validate_rows(df)

        self.assertEqual(
            rejected_df["rejection_reason"].iloc[0], "currency must be a 3-letter ISO code"
        )

    def test_empty_dataframe_returns_two_empty_dataframes(self):
        # LOGIC — empty input must not crash; both outputs must be empty DataFrames
        columns = [
            "trade_id", "desk_code", "trade_date", "instrument_type",
            "notional_amount", "currency", "counterparty_id", "_source_row",
        ]
        df = pd.DataFrame(columns=columns)
        valid_df, rejected_df = validate_rows(df)

        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 0)

    def test_additional_columns_preserved_in_valid_df(self):
        # LOGIC — extra columns beyond the required 7 must pass through untouched
        row = _make_valid_row()
        row["extra_field"] = "some_value"
        df = _df_from_rows(row)
        valid_df, _ = validate_rows(df)

        self.assertIn("extra_field", valid_df.columns)
        self.assertEqual(valid_df["extra_field"].iloc[0], "some_value")

    def test_additional_columns_preserved_in_rejected_df(self):
        # LOGIC — extra columns must also pass through to rejected_df
        row = _make_valid_row(trade_id="")
        row["extra_field"] = "some_value"
        df = _df_from_rows(row)
        _, rejected_df = validate_rows(df)

        self.assertIn("extra_field", rejected_df.columns)

    def test_trade_date_iso_format_only(self):
        # LOGIC — ISO 8601 short date (YYYY-MM-DD) must pass; other common formats must fail
        valid_df_1, rej_1 = validate_rows(_df_from_rows(_make_valid_row(trade_date="2026-06-15")))
        valid_df_2, rej_2 = validate_rows(_df_from_rows(_make_valid_row(trade_date="2026/06/15")))

        self.assertEqual(len(rej_1), 0, "YYYY-MM-DD should be valid")
        self.assertEqual(len(rej_2), 1, "YYYY/MM/DD should be rejected")
        self.assertEqual(
            rej_2["rejection_reason"].iloc[0],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )


if __name__ == "__main__":
    unittest.main()