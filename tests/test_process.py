import unittest
from datetime import datetime
from unittest.mock import patch

import pytz

from process import (
    ET,
    REQUIRED_FIELDS,
    build_pipeline_context,
    deduplicate,
    make_dedup_key,
    now_et,
    run_pipeline,
    transform_record,
    validate_record,
)


class TestNowEt(unittest.TestCase):
    def test_returns_et_timezone(self):
        # LOGIC — timestamp must be in America/Toronto
        ts = now_et()
        self.assertIsNotNone(ts.tzinfo)
        # pytz normalizes the zone; check the UTC offset matches ET
        et = pytz.timezone("America/Toronto")
        expected_offset = et.utcoffset(datetime.now())
        self.assertEqual(ts.utcoffset(), expected_offset)


class TestValidateRecord(unittest.TestCase):
    def test_valid_record_passes(self):
        record = {"id": "1", "source": "sys-a", "payload": {"data": 42}}
        self.assertTrue(validate_record(record))

    def test_missing_id_fails(self):
        record = {"source": "sys-a", "payload": {}}
        self.assertFalse(validate_record(record))

    def test_missing_source_fails(self):
        record = {"id": "1", "payload": {}}
        self.assertFalse(validate_record(record))

    def test_missing_payload_fails(self):
        record = {"id": "1", "source": "sys-a"}
        self.assertFalse(validate_record(record))

    def test_empty_string_id_fails(self):
        record = {"id": "", "source": "sys-a", "payload": {"x": 1}}
        self.assertFalse(validate_record(record))

    def test_none_source_fails(self):
        record = {"id": "1", "source": None, "payload": {"x": 1}}
        self.assertFalse(validate_record(record))

    def test_required_fields_constant_unchanged(self):
        # Guard: if REQUIRED_FIELDS changes, tests need re-evaluation
        self.assertEqual(REQUIRED_FIELDS, {"id", "source", "payload"})


class TestMakeDedupKey(unittest.TestCase):
    def test_key_format(self):
        record = {"id": "42", "source": "feed-x", "payload": {}}
        self.assertEqual(make_dedup_key(record), "feed-x|42")

    def test_key_is_deterministic(self):
        record = {"id": "7", "source": "s", "payload": None}
        self.assertEqual(make_dedup_key(record), make_dedup_key(record))


class TestDeduplicate(unittest.TestCase):
    def _make(self, source, id_):
        return {"id": id_, "source": source, "payload": "p"}

    def test_no_duplicates_returns_all(self):
        records = [self._make("a", "1"), self._make("a", "2"), self._make("b", "1")]
        result = deduplicate(records)
        self.assertEqual(len(result), 3)

    def test_duplicates_are_removed(self):
        records = [self._make("a", "1"), self._make("a", "1"), self._make("a", "2")]
        result = deduplicate(records)
        self.assertEqual(len(result), 2)

    def test_first_occurrence_wins(self):
        r1 = {"id": "1", "source": "s", "payload": "first"}
        r2 = {"id": "1", "source": "s", "payload": "second"}
        result = deduplicate([r1, r2])
        self.assertEqual(result[0]["payload"], "first")

    def test_empty_list(self):
        self.assertEqual(deduplicate([]), [])

    def test_single_record(self):
        r = self._make("x", "99")
        self.assertEqual(deduplicate([r]), [r])

    def test_different_sources_same_id_not_deduped(self):
        r1 = self._make("src-a", "1")
        r2 = self._make("src-b", "1")
        self.assertEqual(len(deduplicate([r1, r2])), 2)


class TestTransformRecord(unittest.TestCase):
    def setUp(self):
        self.ts = datetime(2024, 6, 15, 10, 30, 0, tzinfo=ET)
        self.record = {"id": "5", "source": "feed", "payload": {"v": 1}}

    def test_processed_at_added(self):
        out = transform_record(self.record, self.ts)
        self.assertIn("processed_at", out)

    def test_processed_at_is_iso8601(self):
        out = transform_record(self.record, self.ts)
        # Must parse without error
        parsed = datetime.fromisoformat(out["processed_at"])
        self.assertIsNotNone(parsed)

    def test_dedup_key_added(self):
        out = transform_record(self.record, self.ts)
        self.assertEqual(out["dedup_key"], "feed|5")

    def test_original_fields_preserved(self):
        out = transform_record(self.record, self.ts)
        self.assertEqual(out["id"], "5")
        self.assertEqual(out["source"], "feed")
        self.assertEqual(out["payload"], {"v": 1})


class TestBuildPipelineContext(unittest.TestCase):
    def test_context_keys(self):
        secrets = {"db_password": "***"}
        event = {"records": []}
        ctx = build_pipeline_context(secrets=secrets, event=event)
        self.assertIn("secrets", ctx)
        self.assertIn("event", ctx)
        self.assertIn("started_at", ctx)

    def test_started_at_is_et(self):
        ctx = build_pipeline_context(secrets={}, event={})
        ts: datetime = ctx["started_at"]
        et = pytz.timezone("America/Toronto")
        self.assertEqual(ts.utcoffset(), et.utcoffset(datetime.now()))


class TestRunPipeline(unittest.TestCase):
    def _ctx(self, records):
        return build_pipeline_context(
            secrets={},
            event={"records": records},
        )

    def test_empty_records(self):
        result = run_pipeline(self._ctx([]))
        self.assertEqual(result["records_processed"], 0)
        self.assertEqual(result["records_invalid"], 0)
        self.assertEqual(result["records_deduplicated"], 0)
        self.assertEqual(result["output"], [])

    def test_valid_records_processed(self):
        records = [
            {"id": "1", "source": "s", "payload": "a"},
            {"id": "2", "source": "s", "payload": "b"},
        ]
        result = run_pipeline(self._ctx(records))
        self.assertEqual(result["records_processed"], 2)
        self.assertEqual(result["records_invalid"], 0)

    def test_invalid_records_counted(self):
        records = [
            {"id": "1", "source": "s", "payload": "a"},
            {"id": "", "source": "s", "payload": "b"},   # invalid
        ]
        result = run_pipeline(self._ctx(records))
        self.assertEqual(result["records_processed"], 1)
        self.assertEqual(result["records_invalid"], 1)

    def test_duplicate_records_counted(self):
        records = [
            {"id": "1", "source": "s", "payload": "x"},
            {"id": "1", "source": "s", "payload": "y"},  # duplicate
        ]
        result = run_pipeline(self._ctx(records))
        self.assertEqual(result["records_processed"], 1)
        self.assertEqual(result["records_deduplicated"], 1)

    def test_output_sorted_by_dedup_key(self):
        records = [
            {"id": "b", "source": "s", "payload": 1},
            {"id": "a", "source": "s", "payload": 2},
        ]
        result = run_pipeline(self._ctx(records))
        keys = [r["dedup_key"] for r in result["output"]]
        self.assertEqual(keys, sorted(keys))

    def test_idempotency(self):
        # Running pipeline twice with the same records produces identical output
        records = [
            {"id": "1", "source": "s", "payload": "a"},
            {"id": "2", "source": "t", "payload": "b"},
        ]
        result_a = run_pipeline(self._ctx(records))
        result_b = run_pipeline(self._ctx(records))
        # dedup_keys and ids must match
        keys_a = [r["dedup_key"] for r in result_a["output"]]
        keys_b = [r["dedup_key"] for r in result_b["output"]]
        self.assertEqual(keys_a, keys_b)

    def test_started_at_in_result(self):
        result = run_pipeline(self._ctx([]))
        self.assertIn("started_at", result)


class TestRunPipelineAllInvalid(unittest.TestCase):
    def test_all_invalid(self):
        records = [{"id": "", "source": "", "payload": None}]
        ctx = build_pipeline_context(secrets={}, event={"records": records})
        result = run_pipeline(ctx)
        self.assertEqual(result["records_processed"], 0)
        self.assertEqual(result["records_invalid"], 1)


if __name__ == "__main__":
    unittest.main()