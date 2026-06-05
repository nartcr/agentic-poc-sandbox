# BOILERPLATE
import json
import unittest
from unittest.mock import MagicMock, patch

from secrets import DBCredentials, clear_credentials_cache, get_db_credentials


class TestGetDbCredentials(unittest.TestCase):

    def setUp(self):
        # BOILERPLATE — clear cache before every test to ensure isolation
        clear_credentials_cache()

    def tearDown(self):
        # BOILERPLATE — clean up cache after each test
        clear_credentials_cache()

    def _make_secret_payload(self, overrides: dict = None) -> dict:
        # BOILERPLATE — complete valid secret payload
        base = {
            "host": "aurora-cluster.cluster-abc.ca-central-1.rds.amazonaws.com",
            "port": "5432",
            "dbname": "tradesdb",
            "username": "appuser",
            "password": "s3cur3p@ss",
        }
        if overrides:
            base.update(overrides)
        return base

    def _make_boto_client(self, payload: dict) -> MagicMock:
        # BOILERPLATE — builds a mock secretsmanager client returning the given payload
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(payload)
        }
        return mock_client

    @patch("secrets.boto3.client")
    def test_returns_db_credentials_dataclass(self, mock_boto_client):
        # LOGIC — function must return a DBCredentials instance with correct field values
        payload = self._make_secret_payload()
        mock_boto_client.return_value = self._make_boto_client(payload)

        result = get_db_credentials("prod/aurora/trades")

        self.assertIsInstance(result, DBCredentials)
        self.assertEqual(result.host, "aurora-cluster.cluster-abc.ca-central-1.rds.amazonaws.com")
        self.assertEqual(result.port, "5432")
        self.assertEqual(result.dbname, "tradesdb")
        self.assertEqual(result.username, "appuser")
        self.assertEqual(result.password, "s3cur3p@ss")

    @patch("secrets.boto3.client")
    def test_secretsmanager_called_with_correct_secret_id(self, mock_boto_client):
        # LOGIC — must call get_secret_value with the exact secret_id passed in
        payload = self._make_secret_payload()
        mock_client = self._make_boto_client(payload)
        mock_boto_client.return_value = mock_client

        get_db_credentials("my/secret/id")

        mock_boto_client.assert_called_once_with("secretsmanager")
        mock_client.get_secret_value.assert_called_once_with(SecretId="my/secret/id")

    @patch("secrets.boto3.client")
    def test_result_is_cached_on_second_call(self, mock_boto_client):
        # LOGIC — second call with same secret_id must not call Secrets Manager again
        payload = self._make_secret_payload()
        mock_client = self._make_boto_client(payload)
        mock_boto_client.return_value = mock_client

        first = get_db_credentials("prod/aurora/trades")
        second = get_db_credentials("prod/aurora/trades")

        self.assertIs(first, second)
        mock_client.get_secret_value.assert_called_once()

    @patch("secrets.boto3.client")
    def test_different_secret_ids_are_cached_independently(self, mock_boto_client):
        # LOGIC — cache must be keyed by secret_id; different IDs must each call Secrets Manager
        payload_a = self._make_secret_payload({"dbname": "db_a"})
        payload_b = self._make_secret_payload({"dbname": "db_b"})

        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = [
            {"SecretString": json.dumps(payload_a)},
            {"SecretString": json.dumps(payload_b)},
        ]
        mock_boto_client.return_value = mock_client

        result_a = get_db_credentials("secret/a")
        result_b = get_db_credentials("secret/b")

        self.assertEqual(result_a.dbname, "db_a")
        self.assertEqual(result_b.dbname, "db_b")
        self.assertEqual(mock_client.get_secret_value.call_count, 2)

    @patch("secrets.boto3.client")
    def test_missing_host_raises_runtime_error(self, mock_boto_client):
        # LOGIC — missing "host" key must raise RuntimeError with descriptive message
        payload = self._make_secret_payload()
        del payload["host"]
        mock_boto_client.return_value = self._make_boto_client(payload)

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("host", str(ctx.exception))

    @patch("secrets.boto3.client")
    def test_missing_password_raises_runtime_error(self, mock_boto_client):
        # LOGIC — missing "password" key must raise RuntimeError
        payload = self._make_secret_payload()
        del payload["password"]
        mock_boto_client.return_value = self._make_boto_client(payload)

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("password", str(ctx.exception))

    @patch("secrets.boto3.client")
    def test_missing_port_raises_runtime_error(self, mock_boto_client):
        # LOGIC — missing "port" key must raise RuntimeError
        payload = self._make_secret_payload()
        del payload["port"]
        mock_boto_client.return_value = self._make_boto_client(payload)

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("port", str(ctx.exception))

    @patch("secrets.boto3.client")
    def test_missing_dbname_raises_runtime_error(self, mock_boto_client):
        # LOGIC — missing "dbname" key must raise RuntimeError
        payload = self._make_secret_payload()
        del payload["dbname"]
        mock_boto_client.return_value = self._make_boto_client(payload)

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("dbname", str(ctx.exception))

    @patch("secrets.boto3.client")
    def test_missing_username_raises_runtime_error(self, mock_boto_client):
        # LOGIC — missing "username" key must raise RuntimeError
        payload = self._make_secret_payload()
        del payload["username"]
        mock_boto_client.return_value = self._make_boto_client(payload)

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("username", str(ctx.exception))

    @patch("secrets.boto3.client")
    def test_invalid_json_raises_runtime_error(self, mock_boto_client):
        # LOGIC — non-JSON SecretString must raise RuntimeError
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": "NOT_VALID_JSON{{{"
        }
        mock_boto_client.return_value = mock_client

        with self.assertRaises(RuntimeError) as ctx:
            get_db_credentials("bad/secret")

        self.assertIn("valid JSON", str(ctx.exception))

    def test_clear_credentials_cache_removes_entries(self):
        # LOGIC — clear_credentials_cache must empty the cache so next call re-fetches
        from secrets import _CREDENTIALS_CACHE
        _CREDENTIALS_CACHE["some/secret"] = DBCredentials(
            host="h", port="5432", dbname="d", username="u", password="p"
        )
        self.assertIn("some/secret", _CREDENTIALS_CACHE)

        clear_credentials_cache()

        self.assertEqual(len(_CREDENTIALS_CACHE), 0)

    def test_db_credentials_repr_does_not_contain_password(self):
        # LOGIC — repr must not expose password to prevent accidental log leakage
        creds = DBCredentials(
            host="myhost", port="5432", dbname="mydb",
            username="myuser", password="supersecret"
        )
        rep = repr(creds)
        self.assertNotIn("supersecret", rep)
        self.assertNotIn("myuser", rep)
        self.assertIn("myhost", rep)

    def test_db_credentials_is_frozen(self):
        # LOGIC — DBCredentials must be immutable
        creds = DBCredentials(
            host="h", port="5432", dbname="d", username="u", password="p"
        )
        with self.assertRaises((AttributeError, TypeError)):
            creds.password = "hacked"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()