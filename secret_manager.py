# BOILERPLATE
import json
import logging
from typing import NamedTuple

import boto3

logger = logging.getLogger(__name__)

# LOGIC — in-process cache: keyed by secret_id so that within a single Lambda
# invocation the Secrets Manager API is called at most once per secret.
_CREDENTIALS_CACHE: dict = {}


# LOGIC
class DBCredentials(NamedTuple):
    """Aurora PostgreSQL connection credentials retrieved from Secrets Manager."""

    host: str
    port: int
    username: str
    password: str
    dbname: str


# LOGIC
def get_db_credentials(secret_id: str) -> DBCredentials:
    """Retrieve and parse Aurora DB credentials from AWS Secrets Manager.

    Results are cached in-process for the lifetime of the Lambda invocation.
    No credentials are stored in code or logs.

    Args:
        secret_id: The Secrets Manager secret ID (e.g. "agentic-poc-aurora").

    Returns:
        DBCredentials named tuple with host, port, username, password, dbname.

    Raises:
        KeyError: If the secret JSON is missing any of the expected fields.
        botocore.exceptions.ClientError: If the secret does not exist or the
            execution role lacks secretsmanager:GetSecretValue permission.
    """
    # LOGIC — return cached credentials if already fetched this invocation
    if secret_id in _CREDENTIALS_CACHE:
        logger.debug("Returning cached DB credentials for secret_id=%s", secret_id)
        return _CREDENTIALS_CACHE[secret_id]

    logger.info("Fetching DB credentials from Secrets Manager: secret_id=%s", secret_id)

    # BOILERPLATE — create boto3 client inside function for testability
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — the secret is expected to be a JSON string
    secret_string = response["SecretString"]
    secret_dict = json.loads(secret_string)

    # LOGIC — extract required keys; KeyError raised automatically if any is absent
    credentials = DBCredentials(
        host=secret_dict["host"],
        port=int(secret_dict["port"]),        # cast: Secrets Manager may return str or int
        username=secret_dict["username"],
        password=secret_dict["password"],
        dbname=secret_dict["dbname"],
    )

    # LOGIC — cache for subsequent calls within same invocation
    _CREDENTIALS_CACHE[secret_id] = credentials

    # LOGIC — log host and dbname only; never log username/password (BAC-8)
    logger.info(
        "DB credentials retrieved: host=%s port=%s dbname=%s",
        credentials.host,
        credentials.port,
        credentials.dbname,
    )

    return credentials


# LOGIC
def clear_credentials_cache() -> None:
    """Clear the in-process credentials cache.

    Intended for use in unit tests only.  Not called in production code paths.
    """
    _CREDENTIALS_CACHE.clear()
    logger.debug("Credentials cache cleared.")