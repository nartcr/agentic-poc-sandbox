# BOILERPLATE
import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytz

import notifier


class TestPublishSuccess(unittest.TestCase):
    # LOGIC — success SNS message structure and routing

    def _make_report(self):
        ts = datetime.now(pytz.timezone("America/Toronto")).isoformat()
        return {
            "source_file": "positions/EQTY_2026-06-15_positions.csv",
            "trade_date": "2026-06-15",
            "desk_code": "EQTY",
            "load_timestamp": ts,
            "total_rows_received": 100,
            "rows_loaded": 95,
            "rows_rejected": 5,
            "rows_skipped_duplicate": 0,
        }

    @patch("notifier.boto3.client")
    def test_publish_called_once(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        report = self._make_report()
        notifier.publish_success(report, "arn:aws:sns:us-east-1:123456789012:success-topic")

        mock_sns.publish.assert_called_once()

    @patch("notifier.boto3.client")
    def test_correct_topic_arn_used(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        topic = "arn:aws:sns:us-east-1:123456789012:success-topic"
        notifier.publish_success(self._make_report(), topic)

        call_kwargs = mock_sns.publish.call_args[1]
        self.assertEqual(call_kwargs["TopicArn"], topic)

    @patch("notifier.boto3.client")
    def test_subject_is_trade_load_success(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_success(
            self._make_report(),
            "arn:aws:sns:us-east-1:123456789012:success-topic",
        )

        call_kwargs = mock_sns.publish.call_args[1]
        self.assertEqual(call_kwargs["Subject"], "TRADE_LOAD_SUCCESS")

    @patch("notifier.boto3.client")
    def test_message_is_valid_json_with_all_required_fields(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        report = self._make_report()
        notifier.publish_success(report, "arn:aws:sns:us-east-1:123456789012:success-topic")

        call_kwargs = mock_sns.publish.call_args[1]
        msg = json.loads(call_kwargs["Message"])

        self.assertEqual(msg["event"], "TRADE_LOAD_SUCCESS")
        self.assertEqual(msg["source_file"], report["source_file"])
        self.assertEqual(msg["trade_date"], report["trade_date"])
        self.assertEqual(msg["desk_code"], report["desk_code"])
        self.assertEqual(msg["load_timestamp"], report["load_timestamp"])
        self.assertEqual(msg["total_rows_received"], report["total_rows_received"])
        self.assertEqual(msg["rows_loaded"], report["rows_loaded"])
        self.assertEqual(msg["rows_rejected"], report["rows_rejected"])
        self.assertEqual(msg["rows_skipped_duplicate"], report["rows_skipped_duplicate"])

    @patch("notifier.boto3.client")
    def test_integer_fields_are_integers_in_message(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_success(
            self._make_report(),
            "arn:aws:sns:us-east-1:123456789012:success-topic",
        )

        call_kwargs = mock_sns.publish.call_args[1]
        msg = json.loads(call_kwargs["Message"])
        self.assertIsInstance(msg["total_rows_received"], int)
        self.assertIsInstance(msg["rows_loaded"], int)
        self.assertIsInstance(msg["rows_rejected"], int)
        self.assertIsInstance(msg["rows_skipped_duplicate"], int)

    @patch("notifier.boto3.client")
    def test_sns_exception_raises_runtime_error(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = Exception("SNS unavailable")
        mock_boto_client.return_value = mock_sns

        with self.assertRaises(RuntimeError) as ctx:
            notifier.publish_success(
                self._make_report(),
                "arn:aws:sns:us-east-1:123456789012:success-topic",
            )
        self.assertIn("SNS publish failed", str(ctx.exception))


class TestPublishFailure(unittest.TestCase):
    # LOGIC — failure SNS message structure and routing

    @patch("notifier.boto3.client")
    def test_publish_called_once(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_failure(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            error_message="Database connection failed",
            topic_arn="arn:aws:sns:us-east-1:123456789012:failure-topic",
        )

        mock_sns.publish.assert_called_once()

    @patch("notifier.boto3.client")
    def test_correct_failure_topic_arn_used(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        topic = "arn:aws:sns:us-east-1:123456789012:failure-topic"
        notifier.publish_failure(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            error_message="error",
            topic_arn=topic,
        )

        call_kwargs = mock_sns.publish.call_args[1]
        self.assertEqual(call_kwargs["TopicArn"], topic)

    @patch("notifier.boto3.client")
    def test_subject_is_trade_load_failure(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_failure(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            error_message="error",
            topic_arn="arn:aws:sns:us-east-1:123456789012:failure-topic",
        )

        call_kwargs = mock_sns.publish.call_args[1]
        self.assertEqual(call_kwargs["Subject"], "TRADE_LOAD_FAILURE")

    @patch("notifier.boto3.client")
    def test_message_contains_required_fields(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_failure(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            error_message="DB timeout",
            topic_arn="arn:aws:sns:us-east-1:123456789012:failure-topic",
        )

        call_kwargs = mock_sns.publish.call_args[1]
        msg = json.loads(call_kwargs["Message"])

        self.assertEqual(msg["event"], "TRADE_LOAD_FAILURE")
        self.assertEqual(msg["source_file"], "positions/EQTY_2026-06-15_positions.csv")
        self.assertEqual(msg["error_message"], "DB timeout")
        self.assertIn("failure_timestamp", msg)

    @patch("notifier.boto3.client")
    def test_failure_timestamp_is_et_not_utc(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_boto_client.return_value = mock_sns

        notifier.publish_failure(
            source_file="positions/EQTY_2026-06-15_positions.csv",
            error_message="error",
            topic_arn="arn:aws:sns:us-east-1:123456789012:failure-topic",
        )

        call_kwargs = mock_sns.publish.call_args[1]
        msg = json.loads(call_kwargs["Message"])
        ts_str = msg["failure_timestamp"]

        self.assertNotIn("+00:00", ts_str)
        self.assertFalse(ts_str.endswith("Z"))
        self.assertTrue(
            "-04:00" in ts_str or "-05:00" in ts_str,
            f"Expected ET offset in failure_timestamp, got: {ts_str}",
        )

    @patch("notifier.boto3.client")
    def test_sns_exception_raises_runtime_error(self, mock_boto_client):
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = Exception("Connection refused")
        mock_boto_client.return_value = mock_sns

        with self.assertRaises(RuntimeError) as ctx:
            notifier.publish_failure(
                source_file="positions/EQTY_2026-06-15_positions.csv",
                error_message="error",
                topic_arn="arn:aws:sns:us-east-1:123456789012:failure-topic",
            )
        self.assertIn("SNS publish failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()