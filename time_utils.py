# BOILERPLATE — timezone utility; wraps pytz for consistent ET timestamps

"""
Eastern Time utility for the trade positions pipeline.
All pipeline timestamps must use this module to ensure
regulatory compliance — no UTC timestamps are permitted
in audit records, reports, or SNS messages.
"""

import logging  # BOILERPLATE
from datetime import datetime  # BOILERPLATE

import pytz  # BOILERPLATE

logger = logging.getLogger(__name__)  # BOILERPLATE

_ET_ZONE = pytz.timezone("America/Toronto")  # LOGIC — canonical ET zone, never UTC


def now_et() -> datetime:
    # LOGIC — returns the current moment in Eastern Time (ET/EDT aware of DST)
    """
    Return the current datetime localised to America/Toronto (Eastern Time).

    The returned datetime is timezone-aware and honours DST automatically:
      - UTC offset is -05:00 during Eastern Standard Time (EST)
      - UTC offset is -04:00 during Eastern Daylight Time (EDT)

    All pipeline components that need a timestamp must call this function
    rather than datetime.utcnow() or datetime.now() without a timezone.

    Returns
    -------
    datetime
        Timezone-aware datetime in America/Toronto.
    """
    et_now = datetime.now(_ET_ZONE)
    logger.debug("now_et() called — returning %s", et_now.isoformat())
    return et_now