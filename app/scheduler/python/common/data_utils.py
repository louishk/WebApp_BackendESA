"""
Data conversion and transformation utilities.

Provides type conversion functions for API response data and
record deduplication for batch database operations.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, List, Dict, Optional

import dateutil.parser


def convert_to_bool(value: Any) -> bool:
    """
    Convert string boolean value to Python bool.

    Handles various input types including None, empty strings,
    string "true"/"false", and actual bool values.

    Args:
        value: Value to convert (string, bool, or other)

    Returns:
        bool: Converted boolean value (defaults to False for None/empty)
    """
    if value is None or value == "":
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def convert_to_int(value: Any) -> Optional[int]:
    """
    Convert value to integer, handling None and empty strings.

    Safely converts numeric strings, floats, and integers.
    Returns None for unconvertible values.

    Args:
        value: Value to convert

    Returns:
        int or None: Converted integer or None if conversion fails
    """
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def convert_to_decimal(value: Any) -> Optional[Decimal]:
    """
    Convert value to Decimal, handling None and empty strings.

    Uses string conversion to preserve precision for monetary
    and measurement values.

    Args:
        value: Value to convert

    Returns:
        Decimal or None: Converted Decimal or None if conversion fails
    """
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


def convert_to_datetime(value: Any) -> Optional[datetime]:
    """
    Convert value to Python datetime.

    Handles multiple input types:
    - None/empty string: returns None
    - datetime object: returns as-is (from SQL Server native types)
    - string: parses using dateutil.parser

    Args:
        value: Datetime string, datetime object, or None

    Returns:
        datetime or None: Parsed datetime or None if parsing fails
    """
    if value is None or value == "":
        return None
    # Already a datetime object (from SQL Server)
    if isinstance(value, datetime):
        return value
    try:
        return dateutil.parser.parse(value)
    except (ValueError, TypeError):
        return None


def deduplicate_records(
    data: List[Dict[str, Any]],
    key_columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Deduplicate records by composite key, keeping last occurrence.

    Useful for batch upsert operations where duplicate records
    in the same batch would cause ON CONFLICT errors.

    Args:
        data: List of record dictionaries
        key_columns: List of column names that form the unique key

    Returns:
        Deduplicated list of records (preserves order, keeps last duplicate)

    Example:
        >>> records = [
        ...     {'id': 1, 'site': 'A', 'value': 100},
        ...     {'id': 1, 'site': 'A', 'value': 200},  # duplicate
        ...     {'id': 2, 'site': 'B', 'value': 300},
        ... ]
        >>> deduplicate_records(records, ['id', 'site'])
        [{'id': 1, 'site': 'A', 'value': 200}, {'id': 2, 'site': 'B', 'value': 300}]
    """
    seen = {}
    for record in data:
        key = tuple(record.get(col) for col in key_columns)
        seen[key] = record  # Later records overwrite earlier ones
    return list(seen.values())
