# BOILERPLATE — contract reference module for demo_schema.trade_positions.
# This module does NOT execute any DDL.  It documents the exact column
# definitions, primary key, and ON CONFLICT target used by db_loader so that
# all modules that reference the table draw from a single authoritative source.

# LOGIC — column list in insert order (matches db_loader INSERT statement)
TABLE_SCHEMA = "demo_schema"
TABLE_NAME = "trade_positions"
FULL_TABLE_NAME = f"{TABLE_SCHEMA}.{TABLE_NAME}"

# LOGIC — columns written by db_loader (excludes loaded_at, which defaults server-side)
INSERT_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
]

# LOGIC — composite primary key / ON CONFLICT target
PRIMARY_KEY_COLUMNS = [
    "trade_id",
    "desk_code",
    "trade_date",
]

# LOGIC — all columns including server-managed columns
ALL_COLUMNS = INSERT_COLUMNS + ["loaded_at"]

# LOGIC — idempotent insert statement template used by db_loader.
# Uses %s placeholder compatible with psycopg2.extras.execute_values.
UPSERT_SQL = (
    f"INSERT INTO {FULL_TABLE_NAME} "
    f"({', '.join(INSERT_COLUMNS)}) "
    f"VALUES %s "
    f"ON CONFLICT (trade_id, desk_code, trade_date) DO NOTHING"
)