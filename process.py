import logging
from datetime import datetime
from typing import Any

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")

# LOGIC — required fields every input record must carry
REQUIRED_FIELDS = {"id", "source", "payload"}


def now_et() -> datetime:
    """Return the current wall-clock time in America/Toronto."""
    # LOGIC
    return datetime.now(tz=ET)


def validate_record(record: dict) -> bool:
    """
    Return True only if *record* contains every required field and
    none of those fields is None or an empty string.
    """
    # LOGIC
    for field in REQUIRED_FIELDS:
        value = record.get(field)
        if value is None or value == "":
            logger.warning("Record failed validation — missing field '%s': %s", field, record)
            return False
    return True


def make_dedup_key(record: dict) -> str:
    """
    Composite deduplication key: <source>|<id>

    Records with the same key are considered duplicates; only the first
    occurrence is kept (idempotent across repeated pipeline runs with
    the same input).
    """
    # LOGIC
    return f"{record['source']}|{record['id']}"


def deduplicate(records: list[dict]) -> list[dict]:
    """
    Remove duplicate records based on the composite dedup key.
    Preserves insertion order; first occurrence wins.
    """
    # LOGIC
    seen: set[str] = set()
    unique: list[dict] = []
    for record in records:
        key = make_dedup_key(record)
        if key in seen:
            logger.info("Duplicate record dropped. dedup_key=%s", key)
            continue
        seen.add(key)
        unique.append(record)
    return unique


def transform_record(record: dict, processed_at: datetime) -> dict:
    """
    Enrich a single validated record with pipeline metadata.

    Adds
    ----
    processed_at : ISO-8601 timestamp in ET
    dedup_key    : composite dedup key for traceability
    """
    # LOGIC
    return {
        **record,
        "processed_at": processed_at.isoformat(),
        "dedup_key": make_dedup_key(record),
    }


def build_pipeline_context(secrets: dict, event: dict) -> dict[str, Any]:
    """
    Assemble a self-contained context dictionary that every pipeline
    stage reads from.  Keeps all config in one place; nothing is read
    from the environment inside individual stages.
    """
    # LOGIC
    return {
        "secrets": secrets,
        "event": event,
        "started_at": now_et(),
    }


def run_pipeline(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Orchestrate the full pipeline:

    1. Extract raw records from the event payload.
    2. Validate each record against REQUIRED_FIELDS.
    3. Deduplicate on composite key.
    4. Transform (enrich with metadata).
    5. Return summary.

    Idempotent: running the same event twice produces the same output
    because deduplication is deterministic on the composite key.
    """
    # LOGIC
    event = ctx["event"]
    started_at: datetime = ctx["started_at"]

    raw_records: list[dict] = event.get("records", [])
    logger.info("Pipeline started at %s. raw_record_count=%d", started_at.isoformat(), len(raw_records))

    # Step 1 — validate
    valid_records = [r for r in raw_records if validate_record(r)]
    invalid_count = len(raw_records) - len(valid_records)
    if invalid_count:
        logger.warning("Dropped %d invalid records.", invalid_count)

    # Step 2 — deduplicate (ordered, deterministic)
    unique_records = deduplicate(valid_records)
    duplicate_count = len(valid_records) - len(unique_records)

    # Step 3 — transform
    transformed = [transform_record(r, started_at) for r in unique_records]

    # Step 4 — sort output deterministically so callers get a stable result
    transformed.sort(key=lambda r: r["dedup_key"])

    result = {
        "records_processed": len(transformed),
        "records_invalid": invalid_count,
        "records_deduplicated": duplicate_count,
        "started_at": started_at.isoformat(),
        "output": transformed,
    }

    logger.info(
        "Pipeline complete. processed=%d invalid=%d deduped=%d",
        result["records_processed"],
        result["records_invalid"],
        result["records_deduplicated"],
    )
    return result