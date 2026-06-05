import json
import logging
import os
import psycopg2

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# LOGIC
def write_audit_record(
    source_file: str,
    trade_date: str,
    desk_code: str,
    outcome: str,
    total_rows: int,
    rows_loaded: int,
    rows_rejected: int,
    error_message,
    report_key,
    error_file_key,
    processed_at,
    operator_identity: str,
    credentials,
) -> None:
    """
    Writes one row to app.pipeline_audit for every file processed.
    Uses ON CONFLICT (source_file) DO UPDATE SET ... to support reprocessing.
    Commits immediately — audit write is independent of the trade data transaction.
    """
    # LOGIC — build the upsert SQL targeting app.pipeline_audit
    sql = """
        INSERT INTO app.pipeline_audit (
            source_file,
            trade_date,
            desk_code,
            outcome,
            total_rows,
            rows_loaded,
            rows_rejected,
            error_message,
            report_key,
            error_file_key,
            processed_at,
            operator_identity
        )
        VALUES (
            %(source_file)s,
            %(trade_date)s,
            %(desk_code)s,
            %(outcome)s,
            %(total_rows)s,
            %(rows_loaded)s,
            %(rows_rejected)s,
            %(error_message)s,
            %(report_key)s,
            %(error_file_key)s,
            %(processed_at)s,
            %(operator_identity)s
        )
        ON CONFLICT (source_file) DO UPDATE SET
            trade_date        = EXCLUDED.trade_date,
            desk_code         = EXCLUDED.desk_code,
            outcome           = EXCLUDED.outcome,
            total_rows        = EXCLUDED.total_rows,
            rows_loaded       = EXCLUDED.rows_loaded,
            rows_rejected     = EXCLUDED.rows_rejected,
            error_message     = EXCLUDED.error_message,
            report_key        = EXCLUDED.report_key,
            error_file_key    = EXCLUDED.error_file_key,
            processed_at      = EXCLUDED.processed_at,
            operator_identity = EXCLUDED.operator_identity
    """

    # LOGIC — parameter dict for the upsert
    params = {
        "source_file":       source_file,
        "trade_date":        trade_date,
        "desk_code":         desk_code,
        "outcome":           outcome,
        "total_rows":        total_rows,
        "rows_loaded":       rows_loaded,
        "rows_rejected":     rows_rejected,
        "error_message":     error_message,
        "report_key":        report_key,
        "error_file_key":    error_file_key,
        "processed_at":      processed_at,
        "operator_identity": operator_identity,
    }

    # BOILERPLATE — connect to Aurora with SSL, execute, commit
    conn = None
    try:
        conn = psycopg2.connect(
            host=credentials.host,
            port=credentials.port,
            dbname=credentials.dbname,
            user=credentials.username,
            password=credentials.password,
            sslmode="require",
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        logger.info(
            "Audit record written: source_file=%s outcome=%s",
            source_file,
            outcome,
        )
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        logger.error("Failed to write audit record for %s: %s", source_file, exc)
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass