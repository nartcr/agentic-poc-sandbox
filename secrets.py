# BOILERPLATE
import json
import logging
from dataclasses import dataclass
from typing import Optional

import boto3

# BOILERPLATE
logger = logging.getLogger(__name__)

# LOGIC — module-level cache keyed by secret_id; avoids redundant Secrets Manager calls
_CREDENTIALS_CACHE: dict[str, "DBCredentials"] = {}

_REQUIRED_SECRET_KEYS = ("host", "port", "dbname", "username", "password")


@dataclass(frozen=True, repr=False)
class DBCredentials:
    # LOGIC — typed container for Aurora connection parameters; repr=False prevents credential logging
    host: str
    port: str
    dbname: str
    username: str
    password: str

    def __repr__(self) -> str:
        # LOGIC — deliberately omit password and username from repr to prevent accidental log exposure
        return f"DBCredentials(host={self.host!r}, port={self.port!r}, dbname={self.dbname!r})"


def get_db_credentials(secret_id: str) -> DBCredentials:
    # LOGIC — returns cached credentials if already fetched this invocation
    if secret_id in _CREDENTIALS_CACHE:
        logger.info("Returning cached DB credentials for secret_id=%s", secret_id)
        return _CREDENTIALS_CACHE[secret_id]

    logger.info("Fetching DB credentials from Secrets Manager for secret_id=%s", secret_id)

    # BOILERPLATE — no explicit AWS credentials; relies on Lambda execution role
    client = boto3.client("secretsmanager")

    response = client.get_secret_value(SecretId=secret_id)
    secret_string = response["SecretString"]

    # LOGIC — parse JSON payload from the secret
    try:
        payload: dict = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Secrets Manager secret '{secret_id}' does not contain valid JSON: {exc}"
        ) from exc

    # LOGIC — validate that all required keys are present; raise with specific missing key name
    for key in _REQUIRED_SECRET_KEYS:
        if key not in payload:
            raise RuntimeError(
                f"Secrets Manager secret '{secret_id}' is missing required key: '{key}'"
            )

    credentials = DBCredentials(
        host=payload["host"],
        port=payload["port"],
        dbname=payload["dbname"],
        username=payload["username"],
        password=payload["password"],
    )

    # LOGIC — cache for the lifetime of this Lambda invocation
    _CREDENTIALS_CACHE[secret_id] = credentials
    logger.info("DB credentials fetched and cached for secret_id=%s", secret_id)

    return credentials


def clear_credentials_cache() -> None:
    # LOGIC — allows test teardown and forced re-fetch without restarting the process
    _CREDENTIALS_CACHE.clear()