# BOILERPLATE
import re
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# LOGIC — regex constants compiled once at module load
_DESK_CODE_RE = re.compile(r'^[A-Z0-9_]+$')
_CURRENCY_RE = re.compile(r'^[A-Z]{3}$')
_DATE_FMT = '%Y-%m-%d'

# LOGIC — mandatory fields subject to validation
_MANDATORY_FIELDS = [
    'trade_id',
    'desk_code',
    'trade_date',
    'instrument_type',
    'notional_amount',
    'currency',
    'counterparty_id',
]


def _check_missing(row: pd.Series, field: str) -> 'str | None':
    # LOGIC
    value = row.get(field)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f'missing/malformed {field}: None'
    if isinstance(value, str) and value.strip() == '':
        return f'missing/malformed {field}: empty string'
    return None


def _check_trade_date_format(value) -> 'str | None':
    # LOGIC
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f'missing/malformed trade_date: {value}'
    str_value = str(value).strip()
    try:
        datetime.strptime(str_value, _DATE_FMT)
    except ValueError:
        return f'missing/malformed trade_date: {str_value}'
    return None


def _check_notional_amount(value) -> 'str | None':
    # LOGIC
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f'missing/malformed notional_amount: {value}'
    try:
        numeric = float(value)
    except (ValueError, TypeError):
        return f'missing/malformed notional_amount: {value}'
    if numeric <= 0:
        return f'missing/malformed notional_amount: {value}'
    return None


def _check_currency_format(value) -> 'str | None':
    # LOGIC
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f'missing/malformed currency: {value}'
    str_value = str(value).strip()
    if not _CURRENCY_RE.match(str_value):
        return f'missing/malformed currency: {str_value}'
    return None


def _check_desk_code_format(value) -> 'str | None':
    # LOGIC
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f'missing/malformed desk_code: {value}'
    str_value = str(value).strip()
    if not _DESK_CODE_RE.match(str_value):
        return f'missing/malformed desk_code: {str_value}'
    return None


def _validate_row(row: pd.Series) -> 'str | None':
    # LOGIC — apply field checks in priority order; return first failure
    for field in ['trade_id', 'instrument_type', 'counterparty_id']:
        reason = _check_missing(row, field)
        if reason:
            return reason

    # desk_code: presence + format
    missing_desk = _check_missing(row, 'desk_code')
    if missing_desk:
        return missing_desk
    desk_fmt = _check_desk_code_format(row.get('desk_code'))
    if desk_fmt:
        return desk_fmt

    # trade_date: parseable as YYYY-MM-DD
    date_reason = _check_trade_date_format(row.get('trade_date'))
    if date_reason:
        return date_reason

    # notional_amount: positive numeric
    notional_reason = _check_notional_amount(row.get('notional_amount'))
    if notional_reason:
        return notional_reason

    # currency: exactly 3 uppercase alpha
    currency_reason = _check_currency_format(row.get('currency'))
    if currency_reason:
        return currency_reason

    return None


def validate_rows(df: pd.DataFrame) -> 'tuple[pd.DataFrame, pd.DataFrame]':
    # LOGIC — validate each row; split into valid and rejected DataFrames
    if df.empty:
        logger.warning('validate_rows received an empty DataFrame')
        rejected_df = df.copy()
        rejected_df['rejection_reason'] = pd.Series(dtype=str)
        return df.copy(), rejected_df

    # Apply validation to every row; result is a Series of reason strings or None
    reasons = df.apply(_validate_row, axis=1)

    valid_mask = reasons.isna()
    rejected_mask = ~valid_mask

    valid_df = df[valid_mask].copy().reset_index(drop=True)

    rejected_df = df[rejected_mask].copy()
    rejected_df['rejection_reason'] = reasons[rejected_mask].values
    rejected_df = rejected_df.reset_index(drop=True)

    logger.info(
        'Validation complete: %d valid rows, %d rejected rows',
        len(valid_df),
        len(rejected_df),
    )

    return valid_df, rejected_df