import unittest
from unittest.mock import patch, MagicMock
import json

from transform import load_secret, validate_record, transform_record, process_records

# BOILERPLATE


class TestLoadSecret(unittest.TestCase):
    # LOGIC tests for load_secret

    @patch("transform.boto3.client")
    def test_load_secret_returns_parsed_dict(self, mock_client_cls):
        # LOGIC
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"transform_prefix": "test"})
        }

        result = load_secret("my-secret")

        mock_client.get_secret_value.assert_called_once_with(SecretId="my-secret")
        self.assertEqual(result, {"transform_prefix": "test"})

    @patch("transform.boto3.client")
    def test_load_secret_empty_string_returns_empty_dict(self, mock_client_cls):
        # LOGIC
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_secret_value.return_value = {"SecretString": "{}"}

        result = load_secret("empty-secret")
        self.assertEqual(result, {})

    @patch("transform.boto3.client")
    def test_load_secret_missing_secret_string_defaults_to_empty(self, mock_client_cls):
        # LOGIC
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_secret_value.return_value = {}

        result = load_secret("no-string-secret")
        self.assertEqual(result, {})


class TestValidateRecord(unittest.TestCase):
    # LOGIC tests for validate_record

    def test_valid_record(self):
        self.assertTrue(validate_record({"value": "hello"}))

    def test_missing_value_key(self):
        self.assertFalse(validate_record({"name": "test"}))

    def test_not_a_dict(self):
        self.assertFalse(validate_record("not a dict"))

    def test_none_input(self):
        self.assertFalse(validate_record(None))

    def test_empty_dict(self):
        self.assertFalse(validate_record({}))

    def test_value_can_be_none(self):
        # LOGIC — None is a valid value; the key must exist
        self.assertTrue(validate_record({"value": None}))


class TestTransformRecord(unittest.TestCase):
    # LOGIC tests for transform_record

    def test_default_prefix_used_when_secret_empty(self):
        # LOGIC
        record = {"value": "abc"}
        result = transform_record(record, {})
        self.assertEqual(result["transformed_value"], "processed:abc")
        self.assertEqual(result["original_value"], "abc")
        self.assertIn("processed_at", result)

    def test_custom_prefix_from_secret(self):
        # LOGIC
        record = {"value": "xyz"}
        result = transform_record(record, {"transform_prefix": "CUSTOM"})
        self.assertEqual(result["transformed_value"], "CUSTOM:xyz")

    def test_extra_keys_carried_forward(self):
        # LOGIC — idempotent key passthrough
        record = {"value": "data", "id": 42, "source": "s3"}
        result = transform_record(record, {})
        self.assertEqual(result["id"], 42)
        self.assertEqual(result["source"], "s3")
        self.assertNotIn("value", result)

    def test_value_key_not_in_output(self):
        # LOGIC
        record = {"value": "drop_me"}
        result = transform_record(record, {})
        self.assertNotIn("value", result)

    def test_idempotent_shape(self):
        # LOGIC — calling twice produces the same keys
        record = {"value": "idempotent"}
        r1 = transform_record(record, {"transform_prefix": "p"})
        r2 = transform_record(record, {"transform_prefix": "p"})
        self.assertEqual(set(r1.keys()), set(r2.keys()))
        self.assertEqual(r1["transformed_value"], r2["transformed_value"])
        self.assertEqual(r1["original_value"], r2["original_value"])


class TestProcessRecords(unittest.TestCase):
    # LOGIC tests for process_records

    def test_valid_records_processed(self):
        records = [{"value": "a"}, {"value": "b"}]
        result = process_records(records, {})
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["transformed_value"], "processed:a")
        self.assertEqual(result[1]["transformed_value"], "processed:b")

    def test_invalid_records_skipped(self):
        records = [{"value": "good"}, {"no_value": "bad"}, "not_a_dict"]
        result = process_records(records, {})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["transformed_value"], "processed:good")

    def test_empty_list_returns_empty(self):
        result = process_records([], {})
        self.assertEqual(result, [])

    def test_all_invalid_returns_empty(self):
        result = process_records([{"x": 1}, "foo", 42], {})
        self.assertEqual(result, [])

    def test_secret_prefix_applied_to_all(self):
        records = [{"value": "1"}, {"value": "2"}]
        result = process_records(records, {"transform_prefix": "PRE"})
        self.assertTrue(all(r["transformed_value"].startswith("PRE:") for r in result))