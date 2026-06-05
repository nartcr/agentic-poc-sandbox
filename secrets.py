# BOILERPLATE
import json
import logging

import boto3
import botocore.exceptions

import config

logger = logging.getLogger(__name__)


# LOGIC
def get_db_credentials(secret_id: str) -> dict:
    """
    Retrieve Aurora PostgreSQL credentials from AWS Secrets Manager.

    Returns a dict with keys: host, port, dbname, username, password.
    Raises RuntimeError (with sanitized message) on any boto3 failure.
    """
    try:
        client = boto3.client("secretsmanager", region_name=config.AWS_REGION)
        response = client.get_secret_value(SecretId=secret_id)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: {error_code}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Unexpected error retrieving secret '{secret_id}': {type(exc).__name__}"
        ) from exc

    # LOGIC — parse JSON secret string
    try:
        secret_dict = json.loads(response["SecretString"])
    except (KeyError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' is not valid JSON or missing 'SecretString'."
        ) from exc

    # LOGIC — validate required keys are present
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - set(secret_dict.keys())
    if missing:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required credential fields: {sorted(missing)}"
        )

    return {
        "host": secret_dict["host"],
        "port": secret_dict["port"],
        "dbname": secret_dict["dbname"],
        "username": secret_dict["username"],
        "password": secret_dict["password"],
    }