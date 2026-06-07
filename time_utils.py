# BOILERPLATE
import logging
from datetime import datetime

import pytz

logger = logging.getLogger(__name__)

# LOGIC
_ET_TZ = pytz.timezone("America/Toronto")


def now_et() -> datetime:
    # LOGIC — returns a timezone-aware datetime in America/Toronto (ET).
    # Never returns a naive datetime and never returns UTC+00:00.
    current = datetime.now(_ET_TZ)
    logger.debug("now_et() returning %s", current.isoformat())
    return current


def to_et_isoformat(dt: datetime) -> str:
    # LOGIC — converts any datetime (naive or tz-aware) to ET and returns ISO-8601 string.
    # Naive datetimes are assumed to be UTC; aware datetimes are converted from their
    # existing tz to ET.  This ensures TAC-7: no "+00:00" offsets ever appear in output.
    if dt is None:
        raise ValueError("dt must not be None")

    if dt.tzinfo is None:
        # LOGIC — treat naive as UTC and convert to ET
        dt = pytz.utc.localize(dt).astimezone(_ET_TZ)
    else:
        dt = dt.astimezone(_ET_TZ)

    result = dt.isoformat()
    logger.debug("to_et_isoformat() returning %s", result)
    return result