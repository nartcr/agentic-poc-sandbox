# BOILERPLATE
import datetime
import unittest

import pandas as pd

from src.validator import validate_rows

# LOGIC — canonical column set from the data contract
_REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


def _make_valid_row(**overrides) -> dict:
    """Return a dict representing one valid trade row. Override any field."""
    # LOGIC — baseline valid row matching all validation rules
    row = {
        "trade_id": "T-0001",
        "desk_code": "EQTY",
        "trade_date": "2026-06-01",
        "instrument_type": "EQUITY",
        "notional_amount": "1000000.00",
        "currency": "USD",
        "counterparty_id": "CP-999",
        "_source_file": "positions/EQTY_2026-06-01_positions.csv",
    }
    row.update(overrides)
    return row


def _df_from_rows(*rows) -> pd.DataFrame:
    """Build a DataFrame with all required columns from a list of row dicts."""
    # BOILERPLATE
    return pd.DataFrame(list(rows))


class TestValidateRowsMissingFields(unittest.TestCase):
    """BAC-2 / TAC-2: each required field missing triggers the correct reason."""

    def _assert_single_rejection(self, df: pd.DataFrame, expected_reason: str):
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(valid_df), 0, "Expected no valid rows")
        self.assertEqual(len(rejected_df), 1, "Expected exactly one rejected row")
        self.assertEqual(rejected_df.iloc[0]["_rejection_reason"], expected_reason)
        self.assertEqual(rejected_df.iloc[0]["_source_row_number"], 1)

    def test_missing_trade_id(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(trade_id=""))
        self._assert_single_rejection(df, "trade_id is missing or empty")

    def test_null_trade_id(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(trade_id=None))
        self._assert_single_rejection(df, "trade_id is missing or empty")

    def test_missing_desk_code(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(desk_code=""))
        self._assert_single_rejection(df, "desk_code is missing or empty")

    def test_null_desk_code(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(desk_code=None))
        self._assert_single_rejection(df, "desk_code is missing or empty")

    def test_missing_trade_date(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(trade_date=""))
        self._assert_single_rejection(df, "trade_date is missing or empty")

    def test_null_trade_date(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(trade_date=None))
        self._assert_single_rejection(df, "trade_date is missing or empty")

    def test_missing_instrument_type(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(instrument_type=""))
        self._assert_single_rejection(df, "instrument_type is missing or empty")

    def test_null_instrument_type(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(instrument_type=None))
        self._assert_single_rejection(df, "instrument_type is missing or empty")

    def test_missing_notional_amount(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(notional_amount=""))
        self._assert_single_rejection(df, "notional_amount is missing or empty")

    def test_null_notional_amount(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(notional_amount=None))
        self._assert_single_rejection(df, "notional_amount is missing or empty")

    def test_missing_currency(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(currency=""))
        self._assert_single_rejection(df, "currency is missing or empty")

    def test_null_currency(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(currency=None))
        self._assert_single_rejection(df, "currency is missing or empty")

    def test_missing_counterparty_id(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(counterparty_id=""))
        self._assert_single_rejection(df, "counterparty_id is missing or empty")

    def test_null_counterparty_id(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(counterparty_id=None))
        self._assert_single_rejection(df, "counterparty_id is missing or empty")


class TestValidateRowsFormatErrors(unittest.TestCase):
    """BAC-2 / TAC-2: format rules fire after all presence checks pass."""

    def test_invalid_trade_date_format(self):
        # LOGIC — "not-a-date" should produce the date-format rejection
        df = _df_from_rows(_make_valid_row(trade_date="not-a-date"))
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )

    def test_invalid_trade_date_wrong_separator(self):
        # LOGIC — slash-separated date should fail
        df = _df_from_rows(_make_valid_row(trade_date="2026/06/01"))
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )

    def test_invalid_trade_date_partial(self):
        # LOGIC — partial date string
        df = _df_from_rows(_make_valid_row(trade_date="2026-06"))
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )

    def test_non_numeric_notional_amount(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(notional_amount="abc"))
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "notional_amount is not a valid number",
        )

    def test_notional_amount_with_letters_mixed(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row(notional_amount="1000abc"))
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "notional_amount is not a valid number",
        )

    def test_first_failure_wins_trade_id_beats_date(self):
        # LOGIC — trade_id is missing AND trade_date is invalid; trade_id check fires first
        df = _df_from_rows(_make_valid_row(trade_id="", trade_date="not-a-date"))
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "trade_id is missing or empty",
        )

    def test_first_failure_wins_date_beats_notional(self):
        # LOGIC — trade_date is invalid AND notional is invalid; date check fires first
        df = _df_from_rows(
            _make_valid_row(trade_date="not-a-date", notional_amount="abc")
        )
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 1)
        self.assertEqual(
            rejected_df.iloc[0]["_rejection_reason"],
            "trade_date is not a valid date (expected YYYY-MM-DD)",
        )


class TestValidateRowsValidRow(unittest.TestCase):
    """BAC-1 / TAC-1: a clean row passes through with correct types."""

    def test_valid_row_passes(self):
        # LOGIC
        df = _df_from_rows(_make_valid_row())
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 0, "No rows should be rejected")
        self.assertEqual(len(valid_df), 1, "One row should be valid")

    def test_valid_row_trade_date_cast_to_date(self):
        # LOGIC — TAC-7: trade_date must be a datetime.date, not a string
        df = _df_from_rows(_make_valid_row(trade_date="2026-06-01"))
        valid_df, _ = validate_rows(df)
        val = valid_df.iloc[0]["trade_date"]
        self.assertIsInstance(val, datetime.date)
        self.assertEqual(val, datetime.date(2026, 6, 1))

    def test_valid_row_notional_amount_cast_to_float(self):
        # LOGIC — notional_amount must be float in valid_df
        df = _df_from_rows(_make_valid_row(notional_amount="1234567.89"))
        valid_df, _ = validate_rows(df)
        val = valid_df.iloc[0]["notional_amount"]
        self.assertIsInstance(val, float)
        self.assertAlmostEqual(val, 1234567.89, places=2)

    def test_valid_row_negative_notional(self):
        # LOGIC — negative notional is a valid number
        df = _df_from_rows(_make_valid_row(notional_amount="-500000.00"))
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 0)
        self.assertAlmostEqual(valid_df.iloc[0]["notional_amount"], -500000.0)

    def test_valid_row_zero_notional(self):
        # LOGIC — zero is a valid number
        df = _df_from_rows(_make_valid_row(notional_amount="0"))
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 0)
        self.assertAlmostEqual(valid_df.iloc[0]["notional_amount"], 0.0)

    def test_source_file_column_preserved(self):
        # LOGIC — _source_file column must be carried through to valid_df
        df = _df_from_rows(_make_valid_row())
        valid_df, _ = validate_rows(df)
        self.assertIn("_source_file", valid_df.columns)
        self.assertEqual(
            valid_df.iloc[0]["_source_file"],
            "positions/EQTY_2026-06-01_positions.csv",
        )


class TestValidateRows1000RowClean(unittest.TestCase):
    """BAC-1 / TAC-1: 1,000 clean rows produce zero rejections."""

    def test_1000_clean_rows_zero_rejections(self):
        # LOGIC — generate 1,000 distinct valid rows
        rows = [
            _make_valid_row(
                trade_id=f"T-{i:05d}",
                notional_amount=str(float(i + 1) * 100.0),
                trade_date=f"2026-06-{(i % 28) + 1:02d}",
            )
            for i in range(1000)
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 0, "Expected zero rejections for clean data")
        self.assertEqual(len(valid_df), 1000, "Expected 1,000 valid rows")

    def test_1000_clean_rows_types(self):
        # LOGIC — spot-check type casting on 1,000-row batch
        rows = [
            _make_valid_row(
                trade_id=f"T-{i:05d}",
                notional_amount=str(float(i + 1) * 100.0),
            )
            for i in range(1000)
        ]
        df = pd.DataFrame(rows)
        valid_df, _ = validate_rows(df)
        # All trade_date values should be datetime.date
        for val in valid_df["trade_date"]:
            self.assertIsInstance(val, datetime.date)
        # All notional_amount values should be float
        for val in valid_df["notional_amount"]:
            self.assertIsInstance(val, float)


class TestValidateRows5RowMixedFailures(unittest.TestCase):
    """BAC-2 / TAC-2: 5 distinct rejection types are all captured."""

    def _build_5_row_df(self) -> pd.DataFrame:
        # LOGIC — one row per distinct rejection type
        rows = [
            _make_valid_row(trade_id=""),                          # row 1: missing trade_id
            _make_valid_row(desk_code=""),                         # row 2: missing desk_code
            _make_valid_row(trade_date="not-a-date"),              # row 3: bad date format
            _make_valid_row(notional_amount="abc"),                # row 4: non-numeric notional
            _make_valid_row(counterparty_id=""),                   # row 5: missing counterparty_id
        ]
        return pd.DataFrame(rows)

    def test_5_rejections_count(self):
        # LOGIC
        df = self._build_5_row_df()
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(valid_df), 0)
        self.assertEqual(len(rejected_df), 5)

    def test_5_rejections_distinct_reasons(self):
        # LOGIC — each rejection must have the exact reason string from the spec
        df = self._build_5_row_df()
        _, rejected_df = validate_rows(df)
        reasons = list(rejected_df["_rejection_reason"])
        self.assertIn("trade_id is missing or empty", reasons)
        self.assertIn("desk_code is missing or empty", reasons)
        self.assertIn("trade_date is not a valid date (expected YYYY-MM-DD)", reasons)
        self.assertIn("notional_amount is not a valid number", reasons)
        self.assertIn("counterparty_id is missing or empty", reasons)
        # All reasons are distinct (5 unique strings)
        self.assertEqual(len(set(reasons)), 5)

    def test_5_rejections_source_row_numbers(self):
        # LOGIC — _source_row_number must be 1-based and monotonically increasing
        df = self._build_5_row_df()
        _, rejected_df = validate_rows(df)
        row_numbers = sorted(rejected_df["_source_row_number"].tolist())
        self.assertEqual(row_numbers, [1, 2, 3, 4, 5])

    def test_5_rejections_all_have_rejection_reason(self):
        # LOGIC — no None or empty rejection reason
        df = self._build_5_row_df()
        _, rejected_df = validate_rows(df)
        for reason in rejected_df["_rejection_reason"]:
            self.assertIsNotNone(reason)
            self.assertNotEqual(reason.strip(), "")


class TestValidateRowsMixedValidAndInvalid(unittest.TestCase):
    """Mixed batch: some valid, some invalid."""

    def test_partial_valid_partial_rejected(self):
        # LOGIC — 3 valid rows, 2 invalid rows
        rows = [
            _make_valid_row(trade_id="T-001"),
            _make_valid_row(trade_id=""),                 # rejected: missing trade_id
            _make_valid_row(trade_id="T-003"),
            _make_valid_row(notional_amount="bad"),       # rejected: non-numeric notional
            _make_valid_row(trade_id="T-005"),
        ]
        df = pd.DataFrame(rows)
        valid_df, rejected_df = validate_rows(df)
        self.assertEqual(len(valid_df), 3)
        self.assertEqual(len(rejected_df), 2)

    def test_rejected_rows_contain_original_columns(self):
        # LOGIC — rejected_df must preserve all original columns
        rows = [
            _make_valid_row(trade_id=""),
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df)
        for col in _REQUIRED_COLUMNS:
            self.assertIn(col, rejected_df.columns)
        self.assertIn("_rejection_reason", rejected_df.columns)
        self.assertIn("_source_row_number", rejected_df.columns)

    def test_valid_df_has_no_rejection_columns(self):
        # LOGIC — valid_df must NOT contain _rejection_reason or _source_row_number
        df = pd.DataFrame([_make_valid_row()])
        valid_df, _ = validate_rows(df)
        self.assertNotIn("_rejection_reason", valid_df.columns)
        self.assertNotIn("_source_row_number", valid_df.columns)

    def test_source_row_numbers_reflect_original_position(self):
        # LOGIC — row numbers are based on position in the incoming DataFrame (1-based)
        rows = [
            _make_valid_row(trade_id="T-001"),   # row 1 — valid
            _make_valid_row(trade_id=""),         # row 2 — rejected, source_row_number = 2
            _make_valid_row(trade_id="T-003"),   # row 3 — valid
            _make_valid_row(trade_id=""),         # row 4 — rejected, source_row_number = 4
        ]
        df = pd.DataFrame(rows)
        _, rejected_df = validate_rows(df)
        self.assertEqual(len(rejected_df), 2)
        row_numbers = sorted(rejected_df["_source_row_number"].tolist())
        self.assertEqual(row_numbers, [2, 4])


if __name__ == "__main__":
    unittest.main()