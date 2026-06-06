-- LOGIC: Reference DML for the INSERT executed by audit.start_audit()
-- This statement is executed programmatically via psycopg2 in audit.py.
-- All %s placeholders are bound by psycopg2 at runtime; no values are hardcoded.

INSERT INTO rfdh.audit_log (
    file_name,
    source_file_key,
    status,
    rows_received,
    rows_loaded,
    rows_rejected,
    error_message,
    started_at_et,
    completed_at_et,
    service_identity
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING audit_id;