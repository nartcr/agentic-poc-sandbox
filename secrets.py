# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


def get_db_credentials() -> dict:
    # BOILERPLATE — reads secret ID from environment; no hardcoded values
    secret_id = os.environ["DB_SECRET_ID"]

    # BOILERPLATE — construct Secrets Manager client with no inline credentials
    client = boto3.client("secretsmanager")

    # LOGIC — fetch secret at call time; no module-level caching to support rotation
    logger.info("Fetching DB credentials from Secrets Manager for secret: %s", secret_id)
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — parse the JSON secret string and return structured credential dict
    secret_string = response["SecretString"]
    credentials = json.loads(secret_string)

    # LOGIC — return only the expected keys; caller uses these to build psycopg2.connect()
    return {
        "host": credentials["host"],
        "port": credentials["port"],
        "dbname": credentials["dbname"],
        "username": credentials["username"],
        "password": credentials["password"],
    }