# BOILERPLATE
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict

import boto3

logger = logging.getLogger(__name__)

# LOGIC — in-process cache to avoid redundant Secrets Manager round-trips per Lambda invocation
_secret_cache: Dict[str, Any] = {}


@dataclass(frozen=True)
class DbCredentials:
    # LOGIC — typed DB credential fields matching the DATA CONTRACTS secret JSON keys exactly
    host: str
    port: int
    dbname: str
    username: str
    password: str


@dataclass(frozen=True)
class AppConfig:
    # LOGIC — reserved application-level config from Secrets Manager (no required fields currently)
    raw: Dict[str, Any]


def _get_secret_json(secret_id: str) -> Dict[str, Any]:
    # LOGIC — retrieves and parses secret JSON from Secrets Manager; caches result in-process
    if secret_id in _secret_cache:
        logger.debug("Returning cached secret for secret_id=%s", secret_id)
        return _secret_cache[secret_id]

    logger.debug("Retrieving secret from Secrets Manager: secret_id=%s", secret_id)
    # BOILERPLATE — client created inside function to avoid credential issues at import time
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)

    # LOGIC — secret may be stored as SecretString (JSON text) or SecretBinary
    if "SecretString" in response:
        secret_text = response["SecretString"]
    else:
        # LOGIC — SecretBinary is base64-decoded bytes; decode to UTF-8 string before JSON parse
        secret_text = response["SecretBinary"].decode("utf-8")

    parsed: Dict[str, Any] = json.loads(secret_text)
    _secret_cache[secret_id] = parsed
    logger.debug("Secret retrieved and cached: secret_id=%s keys=%s", secret_id, list(parsed.keys()))
    return parsed


def get_db_credentials(secret_id: str) -> DbCredentials:
    # LOGIC — retrieves DB credentials and constructs typed dataclass; port cast to int because
    # Secrets Manager JSON values for numeric fields may be stored as strings
    parsed = _get_secret_json(secret_id)

    missing = [k for k in ("host", "port", "dbname", "username", "password") if k not in parsed]
    if missing:
        raise KeyError(
            f"DB credentials secret '{secret_id}' is missing required keys: {missing}"
        )

    credentials = DbCredentials(
        host=str(parsed["host"]),
        port=int(parsed["port"]),
        dbname=str(parsed["dbname"]),
        username=str(parsed["username"]),
        password=str(parsed["password"]),
    )
    logger.debug(
        "DbCredentials constructed: host=%s port=%d dbname=%s username=%s",
        credentials.host,
        credentials.port,
        credentials.dbname,
        credentials.username,
    )
    return credentials


def get_app_config(secret_id: str) -> AppConfig:
    # LOGIC — retrieves application-level config secret; returns typed AppConfig wrapping raw dict.
    # Secret may be an empty object {} if not yet populated (reserved for future use per design).
    parsed = _get_secret_json(secret_id)
    app_cfg = AppConfig(raw=parsed)
    logger.debug(
        "AppConfig constructed from secret_id=%s: keys=%s", secret_id, list(parsed.keys())
    )
    return app_cfg