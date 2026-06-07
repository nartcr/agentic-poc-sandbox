# BOILERPLATE
import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)


# LOGIC
def get_db_credentials() -> dict:
    """
    Retrieve Aurora PostgreSQL credentials from AWS Secrets Manager.

    Reads the secret ID from os.environ["DB_SECRET_ID"].
    Returns a dict with keys: host, port, dbname, username, password.
    Never logs the password.
    """
    secret_id = os.environ["DB_SECRET_ID"]  # LOGIC — raises KeyError if absent, as required by TAC-8

    # BOILERPLATE
    client = boto3.client("secretsmanager")

    # LOGIC
    logger.info("Retrieving DB credentials from Secrets Manager for secret_id=%s", secret_id)

    response = client.get_secret_value(SecretId=secret_id)
    secret_str = response["SecretString"]
    secret_json = json.loads(secret_str)

    credentials = {
        "host": secret_json["host"],
        "port": int(secret_json["port"]),
        "dbname": secret_json["dbname"],
        "username": secret_json["username"],
        "password": secret_json["password"],
    }

    # LOGIC — log non-sensitive fields only; never log password
    logger.info(
        "DB credentials retrieved: host=%s dbname=%s",
        credentials["host"],
        credentials["dbname"],
    )

    return credentials