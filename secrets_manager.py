# BOILERPLATE
import json
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# LOGIC — module-level cache: keyed by secret_id, populated on first call per invocation
_cache: dict = {}

# LOGIC — expected keys that must be present in the secret JSON
_REQUIRED_SECRET_KEYS = {"host", "port", "dbname", "username", "password"}


def get_db_credentials(secret_id: str) -> dict:
    """
    Calls secretsmanager:GetSecretValue for secret_id.
    Returns parsed JSON dict with keys: host, port, dbname, username, password.
    Raises RuntimeError if secret is missing or malformed.

    Results are cached for the lifetime of the Lambda invocation to avoid
    repeated API calls. Credential values are never written to logs.
    """
    # LOGIC — return cached result if already fetched during this invocation
    if secret_id in _cache:
        logger.debug(
            "Returning cached credentials for secret_id='%s'.", secret_id
        )
        return _cache[secret_id]

    logger.info(
        "Retrieving database credentials from Secrets Manager for secret_id='%s'.",
        secret_id,
    )

    # BOILERPLATE — boto3 client instantiated at call time, not module import
    client = boto3.client("secretsmanager")

    # LOGIC — fetch the secret value from Secrets Manager
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        logger.error(
            "Failed to retrieve secret '%s' from Secrets Manager. "
            "Error code: %s.",
            secret_id,
            error_code,
        )
        raise RuntimeError(
            f"Could not retrieve secret '{secret_id}' from Secrets Manager "
            f"(error code: {error_code}). "
            "Ensure the Lambda execution role has secretsmanager:GetSecretValue "
            "permission for this secret."
        ) from exc

    # LOGIC — secrets may be stored as a string or binary; handle both
    if "SecretString" in response:
        secret_raw = response["SecretString"]
    elif "SecretBinary" in response:
        secret_raw = response["SecretBinary"].decode("utf-8")
    else:
        logger.error(
            "Secret '%s' contains neither SecretString nor SecretBinary.",
            secret_id,
        )
        raise RuntimeError(
            f"Secret '{secret_id}' returned by Secrets Manager contains "
            "neither SecretString nor SecretBinary. Cannot parse credentials."
        )

    # LOGIC — parse the JSON payload
    try:
        credentials = json.loads(secret_raw)
    except json.JSONDecodeError as exc:
        logger.error(
            "Secret '%s' is not valid JSON. Parse error: %s.",
            secret_id,
            str(exc),
        )
        raise RuntimeError(
            f"Secret '{secret_id}' is not valid JSON. "
            "Ensure the secret value is a JSON object with keys: "
            "host, port, dbname, username, password."
        ) from exc

    # LOGIC — validate that all required keys are present in the secret
    missing_keys = _REQUIRED_SECRET_KEYS - set(credentials.keys())
    if missing_keys:
        logger.error(
            "Secret '%s' is missing required keys: %s.",
            secret_id,
            sorted(missing_keys),
        )
        raise RuntimeError(
            f"Secret '{secret_id}' is missing required keys: "
            f"{sorted(missing_keys)}. "
            "Expected keys: host, port, dbname, username, password."
        )

    # LOGIC — log confirmation of retrieval without exposing any credential values
    logger.info(
        "Successfully retrieved and validated credentials for secret_id='%s'. "
        "Keys present: %s.",
        secret_id,
        sorted(credentials.keys()),
    )

    # LOGIC — cache for the remainder of this Lambda invocation
    _cache[secret_id] = credentials

    return credentials


# LOGIC — utility to clear the cache between invocations in test scenarios
def _clear_cache() -> None:
    """
    Clears the in-memory credential cache.
    Intended for use in unit tests only — not called by production code.
    """
    _cache.clear()