# BOILERPLATE
import json
import logging

import boto3

from config import Config

logger = logging.getLogger(__name__)


def get_db_credentials() -> dict:
    # LOGIC — fetch credentials fresh on every call to support secret rotation
    """Retrieve Aurora PostgreSQL credentials from AWS Secrets Manager.

    Returns a dict with keys: host, port, dbname, username, password.
    Credentials are fetched on each invocation (not cached) to support rotation.
    """
    logger.info(
        "Retrieving database credentials from Secrets Manager: secret_id=%s",
        Config.DB_SECRET_ID,
    )

    # BOILERPLATE — boto3 client constructed per call using region from Config
    client = boto3.client("secretsmanager", region_name=Config.AWS_REGION)

    response = client.get_secret_value(SecretId=Config.DB_SECRET_ID)

    # LOGIC — secret is stored as a JSON string; parse and extract required keys
    secret_string = response["SecretString"]
    secret_dict = json.loads(secret_string)

    credentials = {
        "host": secret_dict["host"],
        "port": secret_dict["port"],
        "dbname": secret_dict["dbname"],
        "username": secret_dict["username"],
        "password": secret_dict["password"],
    }

    logger.info(
        "Database credentials retrieved successfully: host=%s, port=%s, dbname=%s, username=%s",
        credentials["host"],
        credentials["port"],
        credentials["dbname"],
        credentials["username"],
    )

    return credentials