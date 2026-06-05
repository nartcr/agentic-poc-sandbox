-- BOILERPLATE: Schema creation — idempotent, safe to re-run
CREATE SCHEMA IF NOT EXISTS app;

-- LOGIC: Core trade storage table with composite primary key for deduplication
-- Satisfies: BAC-1 (row count integrity), BAC-3 (idempotent via ON CONFLICT target),
--            NFR-3.3 (audit trail support)
CREATE TABLE IF NOT EXISTS app.daily_trades (
    trade_id           VARCHAR(100)     NOT NULL,
    desk_code          VARCHAR(50)      NOT NULL,
    trade_date         DATE             NOT NULL,
    instrument_type    VARCHAR(100)     NOT NULL,
    notional_amount    NUMERIC(24, 6)   NOT NULL,
    currency           CHAR(3)          NOT NULL,
    counterparty_id    VARCHAR(100)     NOT NULL,
    loaded_at          TIMESTAMPTZ      NOT NULL,
    source_file        VARCHAR(500)     NOT NULL,

    -- LOGIC: Composite primary key — enforces deduplication at DB layer
    CONSTRAINT pk_daily_trades PRIMARY KEY (trade_id, desk_code, trade_date),

    -- LOGIC: Named unique constraint — explicitly targeted by ON CONFLICT clause in loader.py
    CONSTRAINT uc_daily_trades_key UNIQUE (trade_id, desk_code, trade_date)
);

-- LOGIC: Index supporting desk + date range queries (most common access pattern)
CREATE INDEX IF NOT EXISTS idx_daily_trades_desk_date
    ON app.daily_trades (desk_code, trade_date);

-- LOGIC: Index supporting trade date range queries and reconciliation
CREATE INDEX IF NOT EXISTS idx_daily_trades_trade_date
    ON app.daily_trades (trade_date);

-- BOILERPLATE: Table comment for data dictionary / OSFI audit documentation
COMMENT ON TABLE app.daily_trades IS
    'Daily trade positions ingested from desk CSV files. '
    'Deduplication enforced on (trade_id, desk_code, trade_date).';

COMMENT ON COLUMN app.daily_trades.trade_id IS 'Unique trade identifier assigned by the trading system.';
COMMENT ON COLUMN app.daily_trades.desk_code IS 'Trading desk code parsed from the source filename.';
COMMENT ON COLUMN app.daily_trades.trade_date IS 'Business date of the trade, parsed from the source filename and validated against row data.';
COMMENT ON COLUMN app.daily_trades.instrument_type IS 'Financial instrument classification (e.g. EQ, FX, RATES).';
COMMENT ON COLUMN app.daily_trades.notional_amount IS 'Notional value of the trade in the trade currency. Precision NUMERIC(24,6).';
COMMENT ON COLUMN app.daily_trades.currency IS 'ISO 4217 three-character currency code.';
COMMENT ON COLUMN app.daily_trades.counterparty_id IS 'Identifier of the counterparty to the trade.';
COMMENT ON COLUMN app.daily_trades.loaded_at IS 'Timestamp when the row was inserted, set in Eastern Time (America/Toronto) by the application layer.';
COMMENT ON COLUMN app.daily_trades.source_file IS 'S3 key of the CSV file from which this row was loaded. Used for audit traceability.';


-- LOGIC: Audit table for OSFI/SOX compliance — one record per source file processed
-- Satisfies: NFR-3.3 (audit trail), BAC-7 (ET timestamps)
CREATE TABLE IF NOT EXISTS app.processing_audit (
    id                 BIGSERIAL        NOT NULL,
    source_file        VARCHAR(500)     NOT NULL,
    desk_code          VARCHAR(50)      NOT NULL,
    trade_date         DATE             NOT NULL,
    status             VARCHAR(20)      NOT NULL,
    total_rows         INTEGER          NOT NULL,
    rows_loaded        INTEGER          NOT NULL,
    rows_rejected      INTEGER          NOT NULL,
    started_at         TIMESTAMPTZ      NOT NULL,
    completed_at       TIMESTAMPTZ,
    error_message      TEXT,
    report_s3_uri      VARCHAR(1000),
    error_file_s3_uri  VARCHAR(1000),

    -- LOGIC: Surrogate primary key for audit table
    CONSTRAINT pk_processing_audit PRIMARY KEY (id),

    -- LOGIC: Named unique constraint on source_file — targeted by ON CONFLICT in audit.py
    --        Ensures re-runs update rather than insert a duplicate audit record
    CONSTRAINT uq_audit_source_file UNIQUE (source_file),

    -- LOGIC: Restrict status to the three valid pipeline outcome values
    CONSTRAINT chk_audit_status CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILURE'))
);

-- LOGIC: Index supporting lookups by desk and trade date (reconciliation queries)
CREATE INDEX IF NOT EXISTS idx_audit_desk_date
    ON app.processing_audit (desk_code, trade_date);

-- LOGIC: Index supporting monitoring queries filtered by pipeline outcome status
CREATE INDEX IF NOT EXISTS idx_audit_status
    ON app.processing_audit (status);

-- BOILERPLATE: Table and column comments for data dictionary / OSFI audit documentation
COMMENT ON TABLE app.processing_audit IS
    'One record per source CSV file processed by the ingestion pipeline. '
    'Used for OSFI/SOX compliance audit trail. Re-runs update existing records via ON CONFLICT.';

COMMENT ON COLUMN app.processing_audit.id IS 'Surrogate primary key — auto-incrementing bigint.';
COMMENT ON COLUMN app.processing_audit.source_file IS 'S3 key of the processed CSV file. Unique — used as natural deduplication key for re-runs.';
COMMENT ON COLUMN app.processing_audit.desk_code IS 'Trading desk code parsed from the source filename.';
COMMENT ON COLUMN app.processing_audit.trade_date IS 'Business date parsed from the source filename.';
COMMENT ON COLUMN app.processing_audit.status IS 'Pipeline outcome: SUCCESS (all rows loaded), PARTIAL (some rejections), FAILURE (pipeline error).';
COMMENT ON COLUMN app.processing_audit.total_rows IS 'Total row count in the source CSV (excluding header).';
COMMENT ON COLUMN app.processing_audit.rows_loaded IS 'Number of rows actually inserted into app.daily_trades (ON CONFLICT skips counted separately).';
COMMENT ON COLUMN app.processing_audit.rows_rejected IS 'Number of rows rejected by the validator and written to the error file.';
COMMENT ON COLUMN app.processing_audit.started_at IS 'Pipeline start timestamp, set in Eastern Time (America/Toronto) by the application layer.';
COMMENT ON COLUMN app.processing_audit.completed_at IS 'Pipeline completion timestamp, set in Eastern Time (America/Toronto). NULL if pipeline did not complete.';
COMMENT ON COLUMN app.processing_audit.error_message IS 'Top-level exception message if status is FAILURE. NULL on SUCCESS or PARTIAL.';
COMMENT ON COLUMN app.processing_audit.report_s3_uri IS 'Full S3 URI of the JSON summary report written for this file. NULL if report was not generated.';
COMMENT ON COLUMN app.processing_audit.error_file_s3_uri IS 'Full S3 URI of the rejection CSV written for this file. NULL if there were no rejected rows.';