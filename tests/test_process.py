import unittest
from unittest.mock import patch
from datetime import datetime
import pytz


class TestCurrentEtTimestamp(unittest.TestCase):

    def test_returns_et_isoformat_string(self):
        # LOGIC — result should be a non-empty ISO-8601 string
        from process import current_et_timestamp
        ts = current_et_timestamp()
        self.assertIsInstance(ts, str)
        self.assertTrue(len(ts) > 0)
        # Should be parseable as a datetime
        parsed = datetime.fromisoformat(ts)
        self.assertIsNotNone(parsed)

    def test_timezone_is_et(self):
        # LOGIC — offset must correspond to ET (UTC-5 or UTC-4 depending on DST)
        from process import current_et_timestamp
        ts = current_et_timestamp()
        parsed = datetime.fromisoformat(ts)
        tz = pytz.timezone("America/Toronto")
        et_now = datetime.now(tz=tz)
        # Both should have the same UTC offset
        self.assertEqual(parsed.utcoffset(), et_now.utcoffset())


class TestProcessEvent(unittest.TestCase):

    def _make_config(self):
        # BOILERPLATE — minimal config stub
        return {"secret_name": "test/secret", "credentials": {}}

    def test_raises_on_non_dict_event(self):
        # LOGIC — non-dict input must raise TypeError
        from process import process_event
        with self.assertRaises(TypeError):
            process_event("not a dict", self._make_config())

    def test_returns_ok_status(self):
        # LOGIC — valid event should return status "ok"
        from process import process_event
        result = process_event({"key": "value"}, self._make_config())
        self.assertEqual(result["status"], "ok")

    def test_input_keys_are_sorted(self):
        # LOGIC — deterministic ordering of input_keys
        from process import process_event
        event = {"z": 1, "a": 2, "m": 3}
        result = process_event(event, self._make_config())
        self.assertEqual(result["input_keys"], ["a", "m", "z"])

    def test_feature_output_contains_key_count(self):
        # LOGIC — feature output should reflect the number of event keys
        from process import process_event
        event = {"x": 1, "y": 2}
        result = process_event(event, self._make_config())
        self.assertEqual(result["feature_output"]["event_key_count"], 2)

    def test_empty_event_is_valid(self):
        # LOGIC — empty dict is a valid event
        from process import process_event
        result = process_event({}, self._make_config())
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["input_keys"], [])


class TestApplyFeature(unittest.TestCase):

    def test_returns_message_and_count(self):
        # LOGIC — internal feature function returns expected keys
        from process import _apply_feature
        output = _apply_feature({"a": 1, "b": 2}, {})
        self.assertIn("message", output)
        self.assertIn("event_key_count", output)
        self.assertEqual(output["event_key_count"], 2)


if __name__ == "__main__":
    unittest.main()