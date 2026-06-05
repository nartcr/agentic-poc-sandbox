import logging
import os
from config import load_config   # BOILERPLATE
from process import process_event  # BOILERPLATE

# BOILERPLATE — configure root logger once at the entry point
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def handler(event: dict, context=None) -> dict:
    # LOGIC — Lambda / container entry point
    logger.info("Handler invoked")
    try:
        config = load_config()
        result = process_event(event, config)
        logger.info("Handler completed successfully")
        return {"statusCode": 200, "body": result}
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        return {"statusCode": 500, "body": {"error": str(exc)}}
    except TypeError as exc:
        logger.error("Invalid input: %s", exc)
        return {"statusCode": 400, "body": {"error": str(exc)}}
    except Exception as exc:  # BOILERPLATE — catch-all so Lambda always returns a structured response
        logger.exception("Unexpected error: %s", exc)
        return {"statusCode": 500, "body": {"error": "internal error"}}