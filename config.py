import logging
import os
import json
import boto3  # BOILERPLATE
import pytz   # BOILERPLATE

# BOILERPLATE — centralised timezone constant used by every module
TZ = pytz.timezone("America/Toronto")

# BOILERPLATE — module-level logger
logger = logging.getLogger(__name__)


def get_secret(secret_name: str) -> dict:
    # BOILERPLATE — read secret from Secrets Manager at runtime; never hardcode credentials
    client = boto3.client("secretsmanager")
    logger.info("Fetching secret: %s", secret_name)
    response = client.get_secret_value(SecretId=secret_name)
    raw = response.get("SecretString", "{}")
    return json.loads(raw)


def load_config() -> dict:
    # LOGIC — build the runtime configuration dict from environment + Secrets Manager
    secret_name = os.environ.get("SECRET_NAME")
    if not secret_name:
        raise EnvironmentError("SECRET_NAME environment variable is not set")
    secret = get_secret(secret_name)
    config = {
        "secret_name": secret_name,
        "credentials": secret,
    }
    logger.debug("Config loaded (secret keys: %s)", list(secret.keys()))
    return config