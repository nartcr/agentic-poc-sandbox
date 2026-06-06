# BOILERPLATE
import logging

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)

# LOGIC — DDL for app.daily_trades per DATA CONTRACTS
_CREATE_DAILY_TRADES = """
CREATE TABLE IF NOT EXISTS app.daily_trades (
    trade_id            VARCHAR(100)        NOT NULL,
    desk_code           VARCHAR(50)         NOT NULL,
    trade_date          DATE                NOT NULL,
    instrument_type     VARCHAR(100)        NOT NULL,
    notional_amount     NUMERIC(24, 6)      NOT NULL,
    currency            CHAR(3)             NOT NULL,
    counterparty_id     VARCHAR(100)        NOT NULL,
    loaded_at           TIMESTAMPTZ         NOT NULL,
    source_file         VARCHAR(500)        NOT NULL,
    CONSTRAINT pk_daily_trades PRIMARY KEY (trade_id, desk_code, trade_date)
);
"""

# LOGIC — supporting indexes on app.daily_trades
_CREATE_IDX_DAILY_TRADES_TRADE_DATE = """
CREATE INDEX IF NOT EXISTS idx_daily_trades_trade_date
    ON app.daily_trades (trade_date);
"""

_CREATE_IDX_DAILY_TRADES_DESK_CODE = """
CREATE INDEX IF NOT EXISTS idx_daily_trades_desk_code
    ON app.daily_trades (desk_code);
"""

# LOGIC — DDL for app.ingestion_audit per DATA CONTRACTS
_CREATE_INGESTION_AUDIT = """
CREATE TABLE IF NOT EXISTS app.ingestion_audit (
    audit_id            BIGSERIAL           PRIMARY KEY,
    source_file         VARCHAR(500)        NOT NULL,
    desk_code           VARCHAR(50)         NOT NULL,
    trade_date          DATE                NOT NULL,
    status              VARCHAR(20)         NOT NULL,
    rows_received       INTEGER             NOT NULL,
    rows_loaded         INTEGER             NOT NULL,
    rows_rejected       INTEGER             NOT NULL,
    error_message       TEXT                NULL,
    processed_at        TIMESTAMPTZ         NOT NULL,
    report_s3_key       VARCHAR(500)        NULL,
    error_file_s3_key   VARCHAR(500)        NULL
);
"""

# LOGIC — supporting indexes on app.ingestion_audit
_CREATE_IDX_AUDIT_SOURCE_FILE = """
CREATE INDEX IF NOT EXISTS idx_ingestion_audit_source_file
    ON app.ingestion_audit (source_file);
"""

_CREATE_IDX_AUDIT_PROCESSED_AT = """
CREATE INDEX IF NOT EXISTS idx_ingestion_audit_processed_at
    ON app.ingestion_audit (processed_at);
"""


def get_connection(credentials: dict) -> psycopg2.extensions.connection:
    """
    # LOGIC — open a psycopg2 connection to Aurora PostgreSQL using SSL.
    All connection parameters come from the passed-in credentials dict;
    no string literals that resemble hostnames, usernames, or passwords appear here.
    """
    host = credentials["host"]
    port = credentials["port"]
    dbname = credentials["dbname"]
    user = credentials["username"]
    password = credentials["password"]

    logger.info(
        "Opening database connection: host=%s port=%s dbname=%s user=%s",
        host,
        port,
        dbname,
        user,
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode="require",
    )
    # LOGIC — default to autocommit=False so the pipeline controls transaction boundaries
    conn.autocommit = False

    # LOGIC — set session timezone to ET per regulatory requirement (BAC-7)
    with conn.cursor() as cur:
        cur.execute("SET TimeZone='America/Toronto'")
    conn.commit()

    logger.info("Database connection established successfully (timezone=America/Toronto).")
    return conn


def ensure_schema(conn: psycopg2.extensions.connection) -> None:
    """
    # LOGIC — idempotent schema setup.
    Creates app.daily_trades and app.ingestion_audit (with all indexes)
    if they do not already exist.  Safe to call on every pipeline invocation.
    """
    ddl_statements = [
        ("app.daily_trades table", _CREATE_DAILY_TRADES),
        ("idx_daily_trades_trade_date", _CREATE_IDX_DAILY_TRADES_TRADE_DATE),
        ("idx_daily_trades_desk_code", _CREATE_IDX_DAILY_TRADES_DESK_CODE),
        ("app.ingestion_audit table", _CREATE_INGESTION_AUDIT),
        ("idx_ingestion_audit_source_file", _CREATE_IDX_AUDIT_SOURCE_FILE),
        ("idx_ingestion_audit_processed_at", _CREATE_IDX_AUDIT_PROCESSED_AT),
    ]

    with conn.cursor() as cur:
        for description, ddl in ddl_statements:
            logger.info("Ensuring schema object exists: %s", description)
            cur.execute(ddl)

    conn.commit()
    logger.info("Schema verification complete — all required objects are present.")