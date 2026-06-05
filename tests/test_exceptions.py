# BOILERPLATE
import unittest

from exceptions import (
    FileReadError,
    ValidationError,
    LoadError,
    ErrorWriteError,
    ReportWriteError,
    NotificationError,
)


class TestFileReadError(unittest.TestCase):
    # LOGIC
    def test_message_contains_key_and_reason(self):
        exc = FileReadError("s3/path/file.csv", "S3 not found")
        self.assertIn("s3/path/file.csv", str(exc))
        self.assertIn("S3 not found", str(exc))

    def test_attributes(self):
        exc = FileReadError("key123", "bad csv")
        self.assertEqual(exc.key, "key123")
        self.assertEqual(exc.reason, "bad csv")

    def test_is_exception(self):
        with self.assertRaises(FileReadError):
            raise FileReadError("k", "r")


class TestValidationError(unittest.TestCase):
    def test_message_and_attribute(self):
        exc = ValidationError("structural failure")
        self.assertIn("structural failure", str(exc))
        self.assertEqual(exc.reason, "structural failure")


class TestLoadError(unittest.TestCase):
    def test_message_and_attribute(self):
        exc = LoadError("db down")
        self.assertIn("db down", str(exc))
        self.assertEqual(exc.reason, "db down")


class TestErrorWriteError(unittest.TestCase):
    def test_message_and_attribute(self):
        exc = ErrorWriteError("s3 write failed")
        self.assertIn("s3 write failed", str(exc))
        self.assertEqual(exc.reason, "s3 write failed")


class TestReportWriteError(unittest.TestCase):
    def test_message_and_attribute(self):
        exc = ReportWriteError("report s3 fail")
        self.assertIn("report s3 fail", str(exc))
        self.assertEqual(exc.reason, "report s3 fail")


class TestNotificationError(unittest.TestCase):
    def test_message_and_attribute(self):
        exc = NotificationError("sns publish fail")
        self.assertIn("sns publish fail", str(exc))
        self.assertEqual(exc.reason, "sns publish fail")


if __name__ == "__main__":
    unittest.main()