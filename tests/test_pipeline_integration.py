import io
import json
import os
import time
import uuid
import datetime
import logging
import unittest
from unittest.mock import MagicMock, patch, call
import pytest
import psycopg2
import psycopg2.extras
import pandas as pd
import pytz

# BOILERPLATE — logging setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BOILERPLATE — test database connection parameters (read from env with defaults)
# ---------------------------------------------------------------------------
TEST_DB_HOST = os.environ.get("TEST_DB_HOST", "localhost")
TEST_DB_PORT = int(os.environ.get("TEST_DB_PORT", "5432"))
TEST_DB_NAME = os.environ.get("TEST_DB_NAME", "postgres")
TEST_DB_USER = os.environ.get("TEST_DB_USER", "postgres")
TEST_DB_PASSWORD = os.environ.get("TEST_DB_PASSWORD", "postgres")

TEST_CREDENTIALS = {
    "host": TEST_DB_HOST,
    "port": TEST_DB_PORT,
    "dbname": TEST_DB_NAME,
    "username": TEST_DB_USER,
    "password": TEST_DB_PASSWORD,
}

ET = pytz.timezone("America/Toronto")

# ---------------------------------------------------------------------------
# BOILERPLATE — S3 key constants used across tests
# ---------------------------------------------------------------------------
SOURCE_KEY = "positions/EQTY_2026-06-01_positions.csv"
ERRORS_PREFIX = "errors/"
REPORTS_PREFIX = "reports/"
S3_BUCKET = "test-bucket"

REQUIRED_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]


# ---------------------------------------------------------------------------
# BOILERPLATE — pytest fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pg_conn():
    """Create and yield a psycopg2 connection to the local test PostgreSQL instance.
    Creates app.daily_trades and app.pipeline_audit tables before the test module
    runs and drops them after.
    """
    conn = psycopg2.connect(
        host=TEST_DB_HOST,
        port=TEST_DB_PORT,
        dbname=TEST_DB_NAME,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
    )
    conn.autocommit = False
    cursor = conn.cursor()

    # BOILERPLATE — create schema and tables for integration tests
    cursor.execute("CREATE SCHEMA IF NOT EXISTS app;")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app.daily_trades (
            trade_id        VARCHAR(100)    NOT NULL,
            desk_code       VARCHAR(50)     NOT NULL,
            trade_date      DATE            NOT NULL,
            instrument_type VARCHAR(100)    NOT NULL,
            notional_amount NUMERIC(28, 10) NOT NULL,
            currency        VARCHAR(10)     NOT NULL,
            counterparty_id VARCHAR(100)    NOT NULL,
            loaded_at       TIMESTAMPTZ     NOT NULL,
            source_file     VARCHAR(500)    NOT NULL,
            CONSTRAINT pk_daily_trades PRIMARY KEY (trade_id, desk_code, trade_date),
            CONSTRAINT uq_daily_trades_dedup UNIQUE (trade_id, desk_code, trade_date)
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS app.pipeline_audit (
            pipeline_run_id    UUID            NOT NULL,
            source_file        VARCHAR(500)    NOT NULL,
            status             VARCHAR(20)     NOT NULL,
            total_rows_received INTEGER,
            rows_loaded        INTEGER,
            rows_rejected      INTEGER,
            error_message      TEXT,
            started_at         TIMESTAMPTZ     NOT NULL,
            completed_at       TIMESTAMPTZ     NOT NULL,
            operator_identity  TEXT            NOT NULL,
            CONSTRAINT pk_pipeline_audit PRIMARY KEY (pipeline_run_id)
        );
    """)
    conn.commit()
    yield conn

    # BOILERPLATE — teardown
    cursor.execute("DROP TABLE IF EXISTS app.daily_trades;")
    cursor.execute("DROP TABLE IF EXISTS app.pipeline_audit;")
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture(autouse=True)
def clean_tables(pg_conn):
    """Truncate both tables before each test to ensure isolation."""
    cursor = pg_conn.cursor()
    cursor.execute("TRUNCATE TABLE app.daily_trades;")
    cursor.execute("TRUNCATE TABLE app.pipeline_audit;")
    pg_conn.commit()
    cursor.close()


# ---------------------------------------------------------------------------
# BOILERPLATE — helper builders
# ---------------------------------------------------------------------------

def _build_clean_csv(num_rows: int, desk_code: str = "EQTY", trade_date: str = "2026-06-01") -> bytes:
    """Build a clean CSV with num_rows valid trade rows."""
    # LOGIC — generate deterministic rows with unique trade_ids
    rows = []
    for i in range(1, num_rows + 1):
        rows.append({
            "trade_id": f"TRD{i:07d}",
            "desk_code": desk_code,
            "trade_date": trade_date,
            "instrument_type": "EQUITY",
            "notional_amount": f"{100000.0 + i:.2f}",
            "currency": "USD",
            "counterparty_id": f"CP{i:05d}",
        })
    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8")


def _build_mixed_csv() -> bytes:
    """Build a CSV with 5 valid rows and 5 invalid rows covering all rejection types."""
    # LOGIC — one row per rejection reason
    rows = [
        # valid rows
        {"trade_id": "TRD0000001", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "100000.00", "currency": "USD",
         "counterparty_id": "CP00001"},
        {"trade_id": "TRD0000002", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "200000.00", "currency": "USD",
         "counterparty_id": "CP00002"},
        {"trade_id": "TRD0000003", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "300000.00", "currency": "USD",
         "counterparty_id": "CP00003"},
        {"trade_id": "TRD0000004", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "400000.00", "currency": "USD",
         "counterparty_id": "CP00004"},
        {"trade_id": "TRD0000005", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "500000.00", "currency": "USD",
         "counterparty_id": "CP00005"},
        # invalid rows
        {"trade_id": "", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "100.00", "currency": "USD",
         "counterparty_id": "CP00006"},
        {"trade_id": "TRD0000007", "desk_code": "", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "100.00", "currency": "USD",
         "counterparty_id": "CP00007"},
        {"trade_id": "TRD0000008", "desk_code": "EQTY", "trade_date": "not-a-date",
         "instrument_type": "EQUITY", "notional_amount": "100.00", "currency": "USD",
         "counterparty_id": "CP00008"},
        {"trade_id": "TRD0000009", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "abc", "currency": "USD",
         "counterparty_id": "CP00009"},
        {"trade_id": "TRD0000010", "desk_code": "EQTY", "trade_date": "2026-06-01",
         "instrument_type": "EQUITY", "notional_amount": "100.00", "currency": "",
         "counterparty_id": "CP00010"},
    ]
    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8")


def _make_s3_get_response(body_bytes: bytes) -> dict:
    """Wrap bytes in a mock S3 get_object response structure."""
    # BOILERPLATE
    return {"Body": io.BytesIO(body_bytes)}


def _make_mock_boto3_client(s3_store: dict, sns_messages: list, secret_payload: dict):
    """Build a factory function that returns appropriately configured mock clients.

    s3_store: dict mapping S3 key -> bytes, mutable (put_object writes here)
    sns_messages: list, mutable (publish appends here)
    secret_payload: dict representing the parsed secret JSON
    """
    # BOILERPLATE — mock client factory

    def _factory(service_name, **kwargs):
        if service_name == "s3":
            mock_s3 = MagicMock()

            def _get_object(Bucket, Key):  # noqa: N803
                if Key not in s3_store:
                    from botocore.exceptions import ClientError
                    error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
                    raise ClientError(error_response, "GetObject")
                return _make_s3_get_response(s3_store[Key])

            def _put_object(Bucket, Key, Body, ContentType=None):  # noqa: N803
                if isinstance(Body, bytes):
                    s3_store[Key] = Body
                else:
                    s3_store[Key] = Body.read() if hasattr(Body, "read") else Body

            mock_s3.get_object.side_effect = _get_object
            mock_s3.put_object.side_effect = _put_object
            return mock_s3

        elif service_name == "sns":
            mock_sns = MagicMock()

            def _publish(TopicArn, Message, Subject=None):  # noqa: N803
                msg_id = str(uuid.uuid4())
                sns_messages.append({"TopicArn": TopicArn, "Message": Message, "Subject": Subject})
                return {"MessageId": msg_id}

            mock_sns.publish.side_effect = _publish
            return mock_sns

        elif service_name == "secretsmanager":
            mock_sm = MagicMock()
            mock_sm.get_secret_value.return_value = {
                "SecretString": json.dumps(secret_payload)
            }
            return mock_sm

        elif service_name == "sts":
            mock_sts = MagicMock()
            mock_sts.get_caller_identity.return_value = {
                "Arn": "arn:aws:iam::123456789012:role/test-role"
            }
            return mock_sts

        else:
            return MagicMock()

    return _factory


def _make_env_vars() -> dict:
    """Return environment variable dict for pipeline Config."""
    # BOILERPLATE
    return {
        "S3_BUCKET": S3_BUCKET,
        "S3_INPUT_PREFIX": "positions/",
        "S3_REPORTS_PREFIX": REPORTS_PREFIX,
        "S3_ERRORS_PREFIX": ERRORS_PREFIX,
        "DB_SECRET_ID": "test/db/secret",
        "SNS_SUCCESS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:success-topic",
        "SNS_FAILURE_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:failure-topic",
        "AUDIT_TABLE": "app.pipeline_audit",
    }


def _run_pipeline_with_mocks(source_key: str, csv_bytes: bytes, pg_conn):
    """Execute run_pipeline end-to-end with mocked AWS and real DB."""
    # LOGIC — wire up mocks and call run_pipeline
    s3_store = {source_key: csv_bytes}
    sns_messages = []
    secret_payload = {
        "host": TEST_DB_HOST,
        "port": str(TEST_DB_PORT),
        "dbname": TEST_DB_NAME,
        "username": TEST_DB_USER,
        "password": TEST_DB_PASSWORD,
    }

    boto3_factory = _make_mock_boto3_client(s3_store, sns_messages, secret_payload)

    env = _make_env_vars()

    with patch.dict(os.environ, env):
        with patch("boto3.client", side_effect=boto3_factory):
            from src.pipeline import run_pipeline
            result = run_pipeline(source_key)

    return result, s3_store, sns_messages


# ---------------------------------------------------------------------------
# TAC-1: Valid 1,000-row file fully loaded with zero errors
# ---------------------------------------------------------------------------

class TestTAC1CleanLoad:
    """TAC-1: 1,000 clean rows load fully with zero rejections."""

    def test_1000_clean_rows_loaded(self, pg_conn):
        # LOGIC — upload 1,000 clean rows, run pipeline, verify DB count = 1,000
        csv_bytes = _build_clean_csv(1000)
        result, s3_store, sns_messages = _run_pipeline_with_mocks(SOURCE_KEY, csv_bytes, pg_conn)

        assert result["total_rows_received"] == 1000
        assert result["rows_loaded"] == 1000
        assert result["rows_rejected"] == 0
        assert result["rows_skipped_duplicate"] == 0

        # LOGIC — verify actual DB row count
        cursor = pg_conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM app.daily_trades WHERE source_file = %s",
            (SOURCE_KEY,)
        )
        db_count = cursor.fetchone()[0]
        cursor.close()
        assert db_count == 1000, f"Expected 1000 rows in DB, got {db_count}"

    def test_report_written_to_s3(self, pg_conn):
        # LOGIC — verify report JSON was written to S3 under reports/ prefix
        csv_bytes = _build_clean_csv(1000)
        result, s3_store, sns_messages = _run_pipeline_with_mocks(SOURCE_KEY, csv_bytes, pg_conn)

        expected_report_key = "reports/EQTY_2026-06-01_positions_report.json"
        assert expected_report_key in s3_store, (
            f"Report key {expected_report_key} not found in s3_store. Keys: {list(s3_store.keys())}"
        )

        report_json = json.loads(s3_store[expected_report_key].decode("utf-8"))
        assert report_json["total_rows_received"] == 1000
        assert report_json["rows_loaded"] == 1000
        assert report_json["rows_rejected"] == 0

    def test_no_error_file_written_for_clean_input(self, pg_conn):
        # LOGIC — error file must NOT be written when there are zero rejections
        csv_bytes = _build_clean_csv(1000)
        result, s