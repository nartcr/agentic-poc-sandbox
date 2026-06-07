import csv
import io
import logging
import re

# BOILERPLATE — logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# LOGIC — exact filename pattern from TDD data contract:
#   {desk_code}_{trade_date}_positions.csv
#   where trade_date is YYYY-MM-DD
#
#   desk_code may contain alphanumeric characters and hyphens but NOT underscores
#   (underscores are the delimiter between components).
#   trade_date is strictly YYYY-MM-DD (digits only, fixed positions).
#   The full filename must end with _positions.csv.
#
#   Regex uses named groups: desk_code, year, month, day.
#   Applied to the basename only (after stripping any S3 prefix path components).
_FILENAME_PATTERN = re.compile(
    r"^(?P<desk_code>[A-Za-z0-9\-]+)"   # desk_code: alphanumeric + hyphens
    r"_"
    r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"  # trade_date: YYYY-MM-DD
    r"_positions\.csv$"
)

# LOGIC — expected CSV columns per data contract
_EXPECTED_COLUMNS = {
    "trade_id",
    "desk_code",
    "trade_date",
    "instrument_type",
    "notional_amount",
    "currency",
    "counterparty_id",
}


def parse_s3_file(s3_client, bucket: str, key: str) -> tuple:
    """
    Downloads the CSV file from S3, validates the filename, and parses all rows.

    Returns:
        (rows, desk_code, trade_date_str)
        rows: list[dict] — raw row dicts keyed by CSV header
        desk_code: str — extracted from filename
        trade_date_str: str — YYYY-MM-DD extracted from filename
    """
    # LOGIC — extract and validate filename from the S3 key
    desk_code, trade_date_str = _parse_filename(key)
    logger.info(
        "Filename validated: key=%s desk_code=%s trade_date=%s",
        key, desk_code, trade_date_str,
    )

    # BOILERPLATE — download S3 object
    logger.info("Downloading s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    raw_bytes = response["Body"].read()
    logger.info("Downloaded %d bytes from s3://%s/%s", len(raw_bytes), bucket, key)

    # LOGIC — decode UTF-8 and parse CSV using DictReader (streaming-safe, no pandas)
    csv_text = raw_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))

    # LOGIC — validate that the header row contains all expected columns
    if reader.fieldnames is None:
        raise ValueError(
            f"CSV file at s3://{bucket}/{key} appears to be empty — no header row found."
        )

    actual_columns = set(f.strip() for f in reader.fieldnames)
    missing_columns = _EXPECTED_COLUMNS - actual_columns
    if missing_columns:
        raise ValueError(
            f"CSV file is missing required columns: {sorted(missing_columns)}. "
            f"Found columns: {sorted(actual_columns)}"
        )

    # LOGIC — read all rows into memory as list of dicts
    rows = []
    for raw_row in reader:
        # Normalize keys by stripping whitespace (defensive against CSV editors adding spaces)
        normalized_row = {k.strip(): v for k, v in raw_row.items() if k is not None}
        rows.append(normalized_row)

    logger.info("Parsed %d data rows from %s", len(rows), key)
    return rows, desk_code, trade_date_str


def _parse_filename(key: str) -> tuple:
    """
    Extracts desk_code and trade_date_str from an S3 key whose basename matches
    the pattern {desk_code}_{trade_date}_positions.csv.

    Args:
        key: S3 object key, possibly with path prefix (e.g. 'incoming/EQTY_2026-06-01_positions.csv')

    Returns:
        (desk_code, trade_date_str) — both strings; trade_date_str is YYYY-MM-DD

    Raises:
        ValueError: if the basename does not match the expected pattern
    """
    # LOGIC — isolate the basename: take everything after the last '/'
    basename = key.split("/")[-1]

    match = _FILENAME_PATTERN.match(basename)
    if match is None:
        raise ValueError(
            f"Filename '{basename}' (from key '{key}') does not match the expected pattern "
            f"'{{desk_code}}_YYYY-MM-DD_positions.csv'. "
            f"desk_code must be alphanumeric/hyphens only; trade_date must be YYYY-MM-DD."
        )

    desk_code = match.group("desk_code")
    year = match.group("year")
    month = match.group("month")
    day = match.group("day")
    trade_date_str = f"{year}-{month}-{day}"

    # LOGIC — additional sanity check: ensure the date components form a valid calendar date
    import datetime  # BOILERPLATE — local import to avoid top-level name shadowing risk
    try:
        datetime.date(int(year), int(month), int(day))
    except ValueError as exc:
        raise ValueError(
            f"Filename '{basename}' contains an invalid calendar date '{trade_date_str}': {exc}"
        ) from exc

    logger.debug("Parsed filename: desk_code=%s trade_date=%s", desk_code, trade_date_str)
    return desk_code, trade_date_str