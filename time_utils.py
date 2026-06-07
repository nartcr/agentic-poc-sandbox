# BOILERPLATE
import datetime
import logging

import pytz

logger = logging.getLogger(__name__)

# BOILERPLATE — single source of truth for ET timezone
_ET_TIMEZONE = pytz.timezone("America/Toronto")


def now_et() -> datetime.datetime:
    # LOGIC — return current timezone-aware datetime localized to America/Toronto
    # Use datetime.now(tz) directly to avoid UTC conversion edge cases
    return datetime.datetime.now(tz=_ET_TIMEZONE)


def to_et(dt: datetime.datetime) -> datetime.datetime:
    # LOGIC — convert any tz-aware datetime to America/Toronto
    if dt.tzinfo is None:
        raise ValueError(
            "to_et() requires a timezone-aware datetime; received naive datetime."
        )
    return dt.astimezone(_ET_TIMEZONE)


def format_et(dt: datetime.datetime) -> str:
    # LOGIC — return ISO 8601 string with UTC offset, e.g. "2026-06-15T21:34:00-04:00"
    # Ensures the datetime is in ET before formatting
    et_dt = to_et(dt)
    return et_dt.isoformat()


def format_et_compact(dt: datetime.datetime) -> str:
    # LOGIC — return compact timestamp string for S3 key construction, e.g. "20260615T213400"
    et_dt = to_et(dt)
    return et_dt.strftime("%Y%m%dT%H%M%S")