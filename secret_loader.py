# BOILERPLATE
import json
import logging

import boto3
import botocore.exceptions

# BOILERPLATE
logger = logging.getLogger(__name__)


def get_db_credentials(secret_id: str) -> dict:
    """Retrieve database credentials from AWS Secrets Manager.

    Parameters
    ----------
    secret_id : str
        The Secrets Manager secret identifier (from pipeline_config.DB_SECRET_ID).

    Returns
    -------
    dict
        Keys: host, port, dbname, username, password.

    Raises
    ------
    RuntimeError
        If the secret cannot be retrieved or the JSON payload is malformed.
    """
    # LOGIC — create a Secrets Manager client; no credentials hardcoded.
    client = boto3.client("secretsmanager")

    # LOGIC — retrieve the secret value; wrap all AWS/network errors in RuntimeError.
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise RuntimeError(
            f"Failed to retrieve secret from Secrets Manager (error code: {error_code})."
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            "Unexpected error while retrieving secret from Secrets Manager."
        ) from exc

    # LOGIC — parse the secret string; Secrets Manager stores credentials as JSON.
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            "Secret retrieved from Secrets Manager contains no 'SecretString' field."
        )

    try:
        payload = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "Secret payload from Secrets Manager is not valid JSON."
        ) from exc

    # LOGIC — validate that all required keys are present in the secret payload.
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - payload.keys()
    if missing:
        raise RuntimeError(
            f"Secret payload is missing required keys: {sorted(missing)}."
        )

    # LOGIC — return only the known credential keys; never log values.
    logger.info("Database credentials retrieved from Secrets Manager successfully.")
    return {
        "host": payload["host"],
        "port": payload["port"],
        "dbname": payload["dbname"],
        "username": payload["username"],
        "password": payload["password"],
    }