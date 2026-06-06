import json
import logging  # BOILERPLATE

import boto3
from botocore.exceptions import ClientError  # BOILERPLATE

logger = logging.getLogger(__name__)  # BOILERPLATE


def get_db_credentials(secret_id: str) -> dict:
    """
    Retrieves Aurora PostgreSQL credentials from AWS Secrets Manager at runtime.

    Returns a dict with keys: host, port (int), dbname, username, password.
    Raises RuntimeError if the secret is missing, inaccessible, or malformed.
    """
    # BOILERPLATE — build Secrets Manager client; no credentials hardcoded
    client = boto3.client("secretsmanager")

    # LOGIC — fetch the secret; surface AWS-level errors as RuntimeError
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise RuntimeError(
            f"Failed to retrieve secret '{secret_id}' from Secrets Manager: "
            f"{error_code} — {exc}"
        ) from exc

    # LOGIC — parse the JSON payload from the secret string
    secret_string = response.get("SecretString")
    if not secret_string:
        raise RuntimeError(
            f"Secret '{secret_id}' exists but contains no SecretString value."
        )

    try:
        raw: dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' SecretString is not valid JSON: {exc}"
        ) from exc

    # LOGIC — validate that all required keys are present before returning
    required_keys = {"host", "port", "dbname", "username", "password"}
    missing = required_keys - raw.keys()
    if missing:
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: {sorted(missing)}"
        )

    # BOILERPLATE — log retrieval success without exposing any credential values
    logger.info("Successfully retrieved database credentials for secret '%s'.", secret_id)

    # LOGIC — cast port to int as specified in the data contract; all other fields remain str
    try:
        port_int = int(raw["port"])
    except (ValueError, TypeError) as exc:
        raise RuntimeError(
            f"Secret '{secret_id}' field 'port' cannot be cast to int: {raw['port']!r}"
        ) from exc

    return {
        "host": raw["host"],
        "port": port_int,
        "dbname": raw["dbname"],
        "username": raw["username"],
        "password": raw["password"],
    }