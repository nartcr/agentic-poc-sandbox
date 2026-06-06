# BOILERPLATE
import datetime
import json
import unittest
from unittest.mock import MagicMock

import pytz

from src.notifier import publish_success, publish_failure

_ET = pytz.timezone("America/Toronto")


def _et_now():
    return datetime.datetime.now(_ET)


def _make_report(rows_inserted=1000, rows_rejected=0, rows_valid=1000):
    return {
        "source_file": "positions/EQTY_2026-06-01_positions.csv",
        "total_rows_received": rows_valid + rows_rejected,
        "rows_loaded": rows_inserted,
        "rows_rejected": rows_rejected,
        "rows_skipped_duplicate": rows_valid - rows_inserted,
        "load_timestamp": "2026-06-01T20:15:33.123456-04:00",
        "desk_code_counts": {"EQTY": rows_valid},
        "notional_min": 100000.0,
        "notional_max": 200000.0,
        "null_rates": {},
        "error_file_key": None,
        "report_key": "reports/EQTY_2026-06-01_positions_report.json",
    }


class TestPublishSuccess(unittest.TestCase):
    def setUp(self):
        self.sns_client = MagicMock()
        self.sns_client.publish.return_value = {"MessageId": "test-message-id-001"}
        self.topic_arn = "arn:aws:sns:us-east-1:123456789012:trade-success"
        self.report = _make_report()

    def test_returns_message_id(self):
        # LOGIC — must return SNS MessageId string
        result = publish_success(self.sns_client, self.topic_arn, self.report)
        self.assertEqual(result, "test-message-id-001")

    def test_sns_publish_called_once(self):
        publish_success(self.sns_client, self.topic_arn, self.report)
        self.sns_client.publish.assert_called_once()

    def test_correct_topic_arn(self):
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        self.assertEqual(call_kwargs["TopicArn"], self.topic_arn)

    def test_subject_is_success_event(self):
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        self.assertEqual(call_kwargs["Subject"], "TRADE_INGESTION_SUCCESS")

    def test_message_is_valid_json(self):
        # LOGIC — message body must be a JSON string
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        message_str = call_kwargs["Message"]
        payload = json.loads(message_str)
        self.assertIsInstance(payload, dict)

    def test_payload_event_field(self):
        # LOGIC — event must equal "TRADE_INGESTION_SUCCESS"
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["event"], "TRADE_INGESTION_SUCCESS")

    def test_payload_contains_all_required_fields(self):
        # LOGIC — all 8 fields from the data contract must be present
        required_fields = [
            "event",
            "source_file",
            "total_rows_received",
            "rows_loaded",
            "rows_rejected",
            "rows_skipped_duplicate",
            "load_timestamp",
            "report_key",
        ]
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        for field in required_fields:
            self.assertIn(field, payload, f"Missing field: {field}")

    def test_payload_source_file_matches_report(self):
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["source_file"], self.report["source_file"])

    def test_payload_total_rows_received(self):
        report = _make_report(rows_inserted=800, rows_rejected=200, rows_valid=800)
        publish_success(self.sns_client, self.topic_arn, report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["total_rows_received"], 1000)

    def test_payload_rows_loaded(self):
        report = _make_report(rows_inserted=750, rows_valid=800, rows_rejected=200)
        publish_success(self.sns_client, self.topic_arn, report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["rows_loaded"], 750)

    def test_payload_rows_rejected(self):
        report = _make_report(rows_inserted=800, rows_rejected=200, rows_valid=800)
        publish_success(self.sns_client, self.topic_arn, report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["rows_rejected"], 200)

    def test_payload_rows_skipped_duplicate(self):
        # LOGIC — skipped = valid - inserted
        report = _make_report(rows_inserted=700, rows_valid=800, rows_rejected=0)
        report["rows_skipped_duplicate"] = 100
        publish_success(self.sns_client, self.topic_arn, report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["rows_skipped_duplicate"], 100)

    def test_payload_report_key(self):
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["report_key"], "reports/EQTY_2026-06-01_positions_report.json")

    def test_payload_load_timestamp_passthrough(self):
        # LOGIC — load_timestamp from report is passed through verbatim
        publish_success(self.sns_client, self.topic_arn, self.report)
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["load_timestamp"], "2026-06-01T20:15:33.123456-04:00")


class TestPublishFailure(unittest.TestCase):
    def setUp(self):
        self.sns_client = MagicMock()
        self.sns_client.publish.return_value = {"MessageId": "test-fail-id-002"}
        self.topic_arn = "arn:aws:sns:us-east-1:123456789012:trade-failure"
        self.source_key = "positions/EQTY_2026-06-01_positions.csv"
        self.error_message = "Connection refused to Aurora host"
        self.failed_at = _et_now()

    def test_returns_message_id(self):
        result = publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        self.assertEqual(result, "test-fail-id-002")

    def test_sns_publish_called_once(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        self.sns_client.publish.assert_called_once()

    def test_correct_topic_arn(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        self.assertEqual(call_kwargs["TopicArn"], self.topic_arn)

    def test_subject_is_failure_event(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        self.assertEqual(call_kwargs["Subject"], "TRADE_INGESTION_FAILURE")

    def test_message_is_valid_json(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertIsInstance(payload, dict)

    def test_payload_contains_all_required_fields(self):
        # LOGIC — 4 fields from the data contract
        required_fields = ["event", "source_file", "error_message", "failed_at"]
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        for field in required_fields:
            self.assertIn(field, payload, f"Missing field: {field}")

    def test_payload_event_field(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["event"], "TRADE_INGESTION_FAILURE")

    def test_payload_source_file(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["source_file"], self.source_key)

    def test_payload_error_message(self):
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(payload["error_message"], self.error_message)

    def test_payload_failed_at_is_et_offset(self):
        # LOGIC — TAC-7: failed_at must be in ET, not UTC
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        failed_at_str = payload["failed_at"]
        self.assertNotIn("+00:00", failed_at_str)
        self.assertFalse(failed_at_str.endswith("Z"))
        self.assertTrue(
            "-04:00" in failed_at_str or "-05:00" in failed_at_str,
            f"Expected ET offset, got: {failed_at_str}",
        )

    def test_payload_failed_at_utc_input_converted_to_et(self):
        # LOGIC — UTC failed_at is converted to ET for the SNS payload
        utc_ts = datetime.datetime(2026, 6, 1, 0, 15, 0, tzinfo=pytz.utc)
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, utc_ts
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        failed_at_str = payload["failed_at"]
        # 00:15 UTC on 2026-06-01 = 20:15 EDT (-04:00) on 2026-05-31
        self.assertIn("-04:00", failed_at_str)
        self.assertNotIn("+00:00", failed_at_str)

    def test_no_extra_fields_in_failure_payload(self):
        # LOGIC — failure payload must contain exactly the 4 contract fields
        publish_failure(
            self.sns_client, self.topic_arn, self.source_key, self.error_message, self.failed_at
        )
        call_kwargs = self.sns_client.publish.call_args[1]
        payload = json.loads(call_kwargs["Message"])
        self.assertEqual(set(payload.keys()), {"event", "source_file", "error_message", "failed_at"})


if __name__ == "__main__":
    unittest.main()