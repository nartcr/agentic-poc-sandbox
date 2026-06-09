# BOILERPLATE
import logging
from datetime import datetime

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — single authoritative timezone constant used by all modules
_ET_ZONE = pytz.timezone("America/Toronto")


def now_et() -> datetime:
    # LOGIC
    """
    Return the current datetime localized to America/Toronto (Eastern Time).

    Observes DST automatically: returns UTC-5 (EST) in winter and
    UTC-4 (EDT) in summer.
    """
    return datetime.now(_ET_ZONE)


def to_et_string(dt: datetime) -> str:
    # LOGIC
    """
    Convert a datetime to an ISO 8601 string with the Eastern Time UTC offset.

    If the input datetime is naive (no tzinfo), it is localized to ET before
    formatting. If it is already tz-aware, it is converted to ET.

    Returns a string like "2026-06-15T19:32:11-04:00" (EDT) or
    "2026-06-15T19:32:11-05:00" (EST).
    """
    if dt.tzinfo is None:
        # LOGIC — localize a naive datetime to ET
        dt_et = _ET_ZONE.localize(dt)
    else:
        # LOGIC — convert a tz-aware datetime to ET
        dt_et = dt.astimezone(_ET_ZONE)

    return dt_et.isoformat()


def et_timestamp_for_key(dt: datetime) -> str:
    # LOGIC
    """
    Return a timestamp string formatted as YYYYMMDD_HHMMSS in Eastern Time.

    Used to build S3 object keys for error files and report files so that
    all key-embedded timestamps are consistent and in ET.

    Example return value: "20260615_193211"
    """
    if dt.tzinfo is None:
        # LOGIC — localize naive datetime to ET before formatting
        dt_et = _ET_ZONE.localize(dt)
    else:
        # LOGIC — convert tz-aware datetime to ET
        dt_et = dt.astimezone(_ET_ZONE)

    return dt_et.strftime("%Y%m%d_%H%M%S")