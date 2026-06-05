import json
import logging
import os

import boto3
import pytz

from process import build_pipeline_context, run_pipeline

# BOILERPLATE
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ET = pytz.timezone("America/Toronto")


def _get_secret(secret_name: str) -> dict:
    # BOILERPLATE — reads secret from Secrets Manager at runtime; never cached to disk
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    raw = response.get("SecretString") or response.get("SecretBinary")
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def handler(event: dict, context) -> dict:
    """
    Lambda / batch entry point.

    Expected environment variables
    --------------------------------
    SECRET_NAME   : name of the Secrets Manager secret that contains runtime config
    """
    # BOILERPLATE
    secret_name = os.environ["SECRET_NAME"]
    logger.info("Loading configuration from Secrets Manager secret: %s", secret_name)

    secrets = _get_secret(secret_name)

    # LOGIC — build context and execute pipeline
    pipeline_ctx = build_pipeline_context(secrets=secrets, event=event)
    result = run_pipeline(pipeline_ctx)

    logger.info("Pipeline completed. records_processed=%d", result["records_processed"])
    return result


if __name__ == "__main__":
    # BOILERPLATE — local convenience runner
    logging.basicConfig(level=logging.INFO)
    handler({}, None)