# BOILERPLATE
import logging
from datetime import datetime

import pytz

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC
_ET_TZ = pytz.timezone("America/Toronto")


def now_et() -> datetime:
    """Return the current datetime localised to America/Toronto (Eastern Time).

    The returned value is timezone-aware and carries the correct EDT/EST offset
    depending on whether daylight saving time is in effect at the moment of the
    call.

    Returns
    -------
    datetime
        Timezone-aware datetime in America/Toronto.
    """
    # LOGIC
    et_now = datetime.now(tz=_ET_TZ)
    logger.debug("now_et() called, returning %s (tzname=%s)", et_now.isoformat(), et_now.tzname())
    return et_now


def format_et(dt: datetime) -> str:
    """Format a timezone-aware datetime as an ISO 8601 string with UTC offset.

    Example output: ``2026-06-01T21:05:33-04:00``

    Parameters
    ----------
    dt:
        A timezone-aware :class:`datetime` object.  Callers are responsible for
        ensuring ``dt`` carries ``tzinfo``; naive datetimes will produce output
        without a UTC offset suffix.

    Returns
    -------
    str
        ISO 8601 formatted string, e.g. ``"2026-06-01T21:05:33-04:00"``.
    """
    # LOGIC
    formatted = dt.isoformat()
    logger.debug("format_et() produced: %s", formatted)
    return formatted