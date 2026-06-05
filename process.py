import logging
from datetime import datetime  # BOILERPLATE
from config import TZ          # BOILERPLATE

# BOILERPLATE
logger = logging.getLogger(__name__)


def current_et_timestamp() -> str:
    # LOGIC — return the current wall-clock time in ET as an ISO-8601 string
    now_et = datetime.now(tz=TZ)
    return now_et.isoformat()


def process_event(event: dict, config: dict) -> dict:
    # LOGIC — core business-logic entry point; extend this function with feature work
    logger.info("Processing event at %s", current_et_timestamp())

    if not isinstance(event, dict):
        raise TypeError("event must be a dict")

    result = {
        "status": "ok",
        "processed_at": current_et_timestamp(),
        "input_keys": sorted(event.keys()),   # deterministic ordering
        "feature_output": _apply_feature(event, config),
    }
    logger.info("Event processed successfully: %s", result["processed_at"])
    return result


def _apply_feature(event: dict, config: dict) -> dict:
    # LOGIC — placeholder for the feature implementation; replace with real logic
    _ = config  # config will be used once real feature logic is added
    return {"message": "feature executed", "event_key_count": len(event)}