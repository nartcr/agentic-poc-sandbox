# BOILERPLATE
import logging
import os

import sqlalchemy
from sqlalchemy import text

from src import config
from src import secrets

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC — DDL for demo_schema and both required tables
_DDL_CREATE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS demo_schema;
"""

# LOGIC — trade_positions table as specified in Data Contracts
_DDL_TRADE_POSITIONS = """
CREATE TABLE IF NOT EXISTS demo_schema.trade_positions (
    trade_id            VARCHAR(100)                 NOT NULL,
    desk_code           VARCHAR(50)                  NOT NULL,
    trade_date          DATE                         NOT NULL,
    instrument_type     VARCHAR(100)                 NOT NULL,
    notional_amount     NUMERIC(28, 8)               NOT NULL,
    currency            VARCHAR(10)                  NOT NULL,
    counterparty_id     VARCHAR(100)                 NOT NULL,
    loaded_at           TIMESTAMP WITH TIME ZONE     NOT NULL,
    CONSTRAINT pk_trade_positions PRIMARY KEY (trade_id, desk_code, trade_date)
);
"""

# LOGIC — explicit unique constraint supporting ON CONFLICT target in loader
_DDL_TRADE_POSITIONS_UNIQUE = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname = 'uq_trade_positions_dedup'
    ) THEN
        ALTER TABLE demo_schema.trade_positions
            ADD CONSTRAINT uq_trade_positions_dedup
            UNIQUE (trade_id, desk_code, trade_date);
    END IF;
END;
$$;
"""

# LOGIC — index on (desk_code, trade_date) for query performance
_DDL_TRADE_POSITIONS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_trade_positions_desk_date
    ON demo_schema.trade_positions (desk_code, trade_date);
"""

# LOGIC — pipeline_audit table as specified in Data Contracts
_DDL_PIPELINE_AUDIT = """
CREATE TABLE IF NOT EXISTS demo_schema.pipeline_audit (
    run_id                  UUID                         NOT NULL,
    s3_key                  VARCHAR(500)                 NOT NULL,
    desk_code               VARCHAR(50)                  NOT NULL,
    trade_date              DATE                         NOT NULL,
    processing_timestamp    TIMESTAMP WITH TIME ZONE     NOT NULL,
    status                  VARCHAR(20)                  NOT NULL,
    total_rows              INTEGER                      NOT NULL,
    rows_inserted           INTEGER                      NOT NULL,
    rows_rejected           INTEGER                      NOT NULL,
    rows_skipped_duplicate  INTEGER                      NOT NULL,
    report_s3_key           VARCHAR(500)                 NULL,
    error_s3_key            VARCHAR(500)                 NULL,
    service_identity        VARCHAR(200)                 NOT NULL,
    CONSTRAINT pk_pipeline_audit PRIMARY KEY (run_id)
);
"""

# LOGIC — indexes on pipeline_audit for audit queries
_DDL_AUDIT_INDEX_DESK_DATE = """
CREATE INDEX IF NOT EXISTS idx_pipeline_audit_desk_date
    ON demo_schema.pipeline_audit (desk_code, trade_date);
"""

_DDL_AUDIT_INDEX_TS = """
CREATE INDEX IF NOT EXISTS idx_pipeline_audit_ts
    ON demo_schema.pipeline_audit (processing_timestamp);
"""


def create_tables(engine) -> None:
    """
    Idempotent DDL execution. Creates demo_schema and both required tables
    with all constraints and indexes. Safe to re-run against an existing database.
    """
    # LOGIC — all DDL executed in a single connection; each statement committed individually
    # because DDL in PostgreSQL is transactional but CREATE INDEX CONCURRENTLY is not —
    # we use non-concurrent here so plain transaction is fine.
    with engine.begin() as conn:
        logger.info("Creating schema demo_schema if not exists.")
        conn.execute(text(_DDL_CREATE_SCHEMA))

        logger.info("Creating table demo_schema.trade_positions if not exists.")
        conn.execute(text(_DDL_TRADE_POSITIONS))

        logger.info("Adding unique constraint uq_trade_positions_dedup if not exists.")
        conn.execute(text(_DDL_TRADE_POSITIONS_UNIQUE))

        logger.info("Creating index idx_trade_positions_desk_date if not exists.")
        conn.execute(text(_DDL_TRADE_POSITIONS_INDEX))

        logger.info("Creating table demo_schema.pipeline_audit if not exists.")
        conn.execute(text(_DDL_PIPELINE_AUDIT))

        logger.info("Creating index idx_pipeline_audit_desk_date if not exists.")
        conn.execute(text(_DDL_AUDIT_INDEX_DESK_DATE))

        logger.info("Creating index idx_pipeline_audit_ts if not exists.")
        conn.execute(text(_DDL_AUDIT_INDEX_TS))

    logger.info("DDL execution complete. All tables and indexes are in place.")


def main() -> None:
    """
    Entry point for one-time DDL initialisation.
    Invoked directly (e.g. python -m src.db_init), not by the Lambda runtime.
    """
    # BOILERPLATE — read config and build connection string from Secrets Manager
    logger.info("Starting database initialisation.")
    connection_string = secrets.get_db_connection_string(config.DB_SECRET_ID, config.DB_NAME)

    # BOILERPLATE — create SQLAlchemy engine; connection string never logged
    engine = sqlalchemy.create_engine(connection_string, pool_pre_ping=True)
    logger.info("SQLAlchemy engine created. Running DDL.")

    create_tables(engine)
    logger.info("Database initialisation complete.")


if __name__ == "__main__":
    # BOILERPLATE — allow running as a script for one-off setup
    logging.basicConfig(level=logging.INFO)
    main()