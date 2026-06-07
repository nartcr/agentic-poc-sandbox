# BOILERPLATE
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# BOILERPLATE — timezone constant reused across all calls
_ET_TZ = pytz.timezone("America/Toronto")


def now_et() -> datetime:
    # LOGIC — return current datetime localized to America/Toronto (ET)
    # Uses datetime.now(tz) directly to produce an offset-aware datetime in ET
    return datetime.now(_ET_TZ)


def to_et_string(dt: datetime) -> str:
    # LOGIC — return ISO-8601 formatted string with ET offset
    # Converts to ET if the datetime has a different tzinfo (e.g. UTC from external source)
    # If dt is naive (no tzinfo), localize it as ET
    if dt.tzinfo is None:
        localized = _ET_TZ.localize(dt)
    else:
        localized = dt.astimezone(_ET_TZ)

    return localized.isoformat()